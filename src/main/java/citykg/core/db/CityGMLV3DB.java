package citykg.core.db;


import citykg.core.config.CityKGDBConfig;
import citykg.core.factory.AuxNodeLabels;
import citykg.core.factory.AuxPropNames;
import citykg.core.factory.EdgeTypes;
import citykg.core.ref.Neo4jRef;
import citykg.utils.BatchUtils;
import com.github.davidmoten.rtree.geometry.Geometries;
import org.citygml4j.core.model.CityGMLVersion;
import org.citygml4j.core.model.core.*;
import org.citygml4j.xml.CityGMLContext;
import org.citygml4j.xml.CityGMLContextException;
import org.citygml4j.xml.module.citygml.CoreModule;
import org.citygml4j.xml.reader.*;
import org.citygml4j.xml.writer.CityGMLOutputFactory;
import org.citygml4j.xml.writer.CityGMLWriteException;
import org.citygml4j.xml.writer.CityGMLWriter;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlobjects.gml.model.base.AbstractGML;
import org.xmlobjects.gml.model.base.AbstractInlineOrByReferenceProperty;
import org.xmlobjects.gml.model.base.AbstractReference;
import org.xmlobjects.gml.model.feature.BoundingShape;
import org.xmlobjects.gml.model.geometry.Envelope;
import org.xmlobjects.gml.util.EnvelopeOptions;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;

public class CityGMLV3DB extends CityKGDB {
    protected final static Logger logger = LoggerFactory.getLogger(CityGMLV3DB.class);

    public CityGMLV3DB(String configPath) {
        this(new CityKGDBConfig(configPath));
    }

    public CityGMLV3DB(CityKGDBConfig config) {
        super(config);
        uuidClasses = Set.of(AbstractFeature.class);
        idClasses = Set.of(AbstractGML.class);
        hrefClasses = Set.of(AbstractReference.class, EngineeringCRSProperty.class,
                AbstractInlineOrByReferenceProperty.class); // All implementing AssociationAttributes
    }

    @Override
    protected Neo4jRef mapCityFile(String filePath, int partitionIndex, boolean connectToRoot) {
        final Neo4jRef[] cityModelRef = {null};
        try {
            CityKGDBConfig cityGMLConfig = (CityKGDBConfig) config;
            if (!cityGMLConfig.CITYGML_VERSION.equals("v3_0")) {
                logger.warn("Found CityGML version {}, expected version {}",
                        cityGMLConfig.CITYGML_VERSION, "v3_0");
            }
            dbStats.startTimer();
            CityGMLContext context = CityGMLContext.newInstance();
            CityGMLInputFactory in = context.createCityGMLInputFactory()
                    .withChunking(ChunkOptions.chunkByFeatures().skipCityModel(false));
            Path file = Path.of(filePath);
            logger.info("Reading CityGML v3.0 file {} chunk-wise into main memory", filePath);

            // Ids of top-level features with no existing bounding shapes
            List<Neo4jRef> topLevelNoBbbox = Collections.synchronizedList(new ArrayList<>());

            // Multi-threading
            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            AtomicLong tlCount = new AtomicLong();
            try (CityGMLReader reader = in.createCityGMLReader(file)) {
                while (reader.hasNext()) {
                    CityGMLChunk chunk = reader.nextChunk();

                    executorService.submit((Callable<Void>) () -> {
                        AbstractFeature feature = chunk.build();
                        tlCount.getAndIncrement();
                        boolean toUpdateBboxTL = preProcessMapping(feature);
                        Neo4jRef graphRef = (Neo4jRef) this.map(feature,
                                AuxNodeLabels.__PARTITION_INDEX__.name() + partitionIndex);
                        postProcessMapping(toUpdateBboxTL, feature, graphRef, partitionIndex, topLevelNoBbbox);
                        logger.info("Mapped {} top-level features", tlCount.get());

                        if (feature instanceof CityModel) {
                            if (cityModelRef[0] != null)
                                throw new RuntimeException("Found multiple CityModel objects in one file");
                            cityModelRef[0] = graphRef;
                            if (connectToRoot) {
                                //  Connect MAPPER root node with this CityModel
                                connectCityModelToRoot(graphRef, Map.of(
                                        AuxPropNames.COLLECTION_INDEX.toString(), partitionIndex,
                                        AuxPropNames.COLLECTION_MEMBER_TYPE.toString(), CityModel.class.getName()
                                ));
                            }
                        }
                        return null;
                    });
                }
            }

            Neo4jDB.finishThreads(executorService, config.MAPPER_CONCURRENT_TIMEOUT);
            dbStats.stopTimer("Map input file [" + partitionIndex + "]");

            dbStats.startTimer();
            setIndexesIfNew();
            resolveXLinks(resolveLinkRules(), correctLinkRules(), partitionIndex);
            dbStats.stopTimer("Resolve links of input file [" + partitionIndex + "]");

            dbStats.startTimer();
            logger.info("Calculate and map bounding boxes of top-level features");
            calcTLBbox(topLevelNoBbbox, partitionIndex);
            setIndexesIfNew(); // In case new bbox properties were added
            dbStats.stopTimer("Calculate and map bounding boxes of top-level features");

            logger.info("Finished mapping file {}", filePath);
        } catch (CityGMLContextException | CityGMLReadException e) {
            throw new RuntimeException(e);
        }
        return cityModelRef[0];
    }

    @Override
    protected boolean preProcessMapping(Object chunk) {
        boolean toUpdateBboxTL = toUpdateBboxTL(chunk);
        if (toUpdateBboxTL) ((AbstractCityObject) chunk).setBoundedBy(null);
        return toUpdateBboxTL;
    }

    @Override
    protected void postProcessMapping(boolean toUpdateBboxTL, Object chunk, Neo4jRef graphRef, int partitionIndex, List<Neo4jRef> topLevelNoBbox) {
        if (!isTopLevel(chunk)) return;
        if (toUpdateBboxTL) topLevelNoBbox.add(graphRef);
        else {
            BoundingShape boundingShape = ((AbstractCityObject) chunk).getBoundedBy();
            if (boundingShape == null) {
                logger.debug("Bounding shape does not exist for top-level feature {}, will be calculated after XLink resolution", chunk.getClass().getName());
                topLevelNoBbox.add(graphRef);
                return;
            }
            addToRtree(boundingShape, graphRef, partitionIndex);
        }
    }

    @Override
    protected Class<?> getCityModelClass() {
        return CityModel.class;
    }

    @Override
    protected boolean toUpdateBboxTL(Object chunk) {
        if (!isTopLevel(chunk)) return false;
        AbstractCityObject aco = (AbstractCityObject) chunk;
        return aco.getBoundedBy() == null
                || aco.getGeometryInfo(true).hasImplicitGeometries()
                || aco.getGeometryInfo(true).hasLodImplicitGeometries();
    }

    @Override
    protected void calcTLBbox(List<Neo4jRef> topLevelNoBbox, int partitionIndex) {
        if (topLevelNoBbox == null) return;
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<List<Neo4jRef>> batches = BatchUtils.toBatches(topLevelNoBbox, 10 * config.MAPPER_TOPLEVEL_BATCH_SIZE);
        batches.forEach(batch -> executorService.submit((Callable<Void>) () -> {
            try (Transaction tx = graphDb.beginTx()) {
                batch.forEach(graphRef -> {
                    // Calculate bounding shape
                    Node topLevelNode = graphRef.getRepresentationNode(tx);
                    AbstractCityObject aco = (AbstractCityObject) toObject(topLevelNode, null);
                    aco.computeEnvelope(EnvelopeOptions.defaults().setEnvelopeOnFeatures(true));
                    BoundingShape boundingShape = aco.getBoundedBy();
                    if (boundingShape == null) {
                        logger.warn("Bounding shape not found for top-level feature {}, ignoring", aco.getClass().getName());
                        return;
                    }
                    Node boundingShapeNode;
                    try {
                        boundingShapeNode = map(tx, boundingShape, new IdentityHashMap<>(), AuxNodeLabels.__PARTITION_INDEX__.name() + partitionIndex);
                    } catch (IllegalAccessException e) {
                        logger.error("Error mapping bounding shape for top-level feature {}, {}",
                                aco.getClass().getName(), e.getMessage());
                        throw new RuntimeException(e);
                    }
                    topLevelNode.createRelationshipTo(boundingShapeNode, EdgeTypes.boundedBy);

                    // Add top-level features to RTree index
                    addToRtree(boundingShape, graphRef, partitionIndex);
                });
                tx.commit();
            }
            return null;
        }));
        Neo4jDB.finishThreads(executorService, config.MAPPER_CONCURRENT_TIMEOUT);
    }

    @Override
    protected void addToRtree(Object boundingShape, Neo4jRef graphRef, int partitionIndex) {
        if (boundingShape instanceof BoundingShape) {
            Envelope envelope = ((BoundingShape) boundingShape).getEnvelope();
            if (envelope == null) {
                logger.warn("Envelope not found for top-level feature, ignoring");
                return;
            }
            double lowerX = envelope.getLowerCorner().getValue().get(0);
            double lowerY = envelope.getLowerCorner().getValue().get(1);
            double upperX = envelope.getUpperCorner().getValue().get(0);
            double upperY = envelope.getUpperCorner().getValue().get(1);
            synchronized (rtrees[partitionIndex]) {
                rtrees[partitionIndex] = rtrees[partitionIndex].add(graphRef, Geometries.rectangle(
                        lowerX, lowerY,
                        upperX, upperY
                ));
                // TODO Also use Geometries.rectangleGeographic(..) for lat and lon values
            }
        }
    }

    @Override
    protected Node getTopLevelListNode(Node cityModelNode) {
        // TODO
        return null;
    }

    @Override
    protected boolean isParentOfTopLevel(Node parentTopLevelNode) {
        return parentTopLevelNode.hasLabel(Label.label(AbstractCityObjectProperty.class.getName()))
                && isTopLevel(parentTopLevelNode.getSingleRelationship(EdgeTypes.object, Direction.OUTGOING).getEndNode());
    }

    @Override
    protected boolean isTopLevel(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(), false)
                .anyMatch(label -> {
                    try {
                        return AbstractCityObject.class.isAssignableFrom(Class.forName(label.name()));
                    } catch (ClassNotFoundException e) {
                        return false;
                    }
                });
    }

    @Override
    protected boolean isTopLevel(Object object) {
        return object instanceof AbstractCityObject;
    }

    @Override
    public BiConsumer<Node, Object> handleOriginXLink() {
        return (node, obj) -> {
            // Only accept nodes whose "object" has more than one incoming "object" edge
            if (node.hasRelationship(Direction.OUTGOING, EdgeTypes.object)
                    && node.getSingleRelationship(EdgeTypes.object, Direction.OUTGOING).getEndNode()
                    .getRelationships(Direction.INCOMING, EdgeTypes.object)
                    .stream().count() <= 1) return;
            if (obj instanceof AbstractInlineOrByReferenceProperty<?> xlink) {
                if (xlink.getObject() instanceof AbstractGML child) {
                    xlink.setHref("#" + child.getId());
                    xlink.setReferencedObject(null);
                } else {
                    logger.warn("Could not find ID of referenced XLink object {}, ignoring",
                            xlink.getClass().getSimpleName());
                }
            } else {
                logger.warn("Object {} is not an XLink, ignoring", obj.getClass().getSimpleName());
            }
        };
    }

    @Override
    public void exportCityFile() {
        CityKGDBConfig cityGMLConfig = (CityKGDBConfig) config;
        exportCityFile(cityGMLConfig.CITYGML_EXPORT_PARTITION, cityGMLConfig.CITYGML_EXPORT_BBOX);
    }

    // TODO Solve: appTarget of exported Appearances are empty (<app:target>#PolyID58908_1911_59781_62378</app:target> -> <app:target/>)
    @Override
    public void exportCityFile(int partitionIndex, double[] topLevelBbox) {
        try (Transaction tx = graphDb.beginTx()) {
            // Get the CityModel node
            Node cityModelNode = tx.findNodes(Label.label(CityModel.class.getName()))
                    .stream()
                    .filter(node -> node.hasLabel(
                            Label.label(AuxNodeLabels.__PARTITION_INDEX__.toString() + partitionIndex)))
                    .findFirst()
                    .get();

            CityModel cityModel = (CityModel) toObject(cityModelNode, handleOriginXLink(), topLevelBbox, this::isParentOfTopLevel);

            CityGMLContext context = CityGMLContext.newInstance();
            CityGMLVersion version = CityGMLVersion.v3_0;
            CityGMLOutputFactory out = context.createCityGMLOutputFactory(version);
            String exportFilePath = ((CityKGDBConfig) config).CITYGML_EXPORT_PATH;
            Path output = Path.of(exportFilePath);
            try (CityGMLWriter writer = out.createCityGMLWriter(output, StandardCharsets.UTF_8.name())) {
                writer.withIndent("  ")
                        .withDefaultSchemaLocations()
                        .withDefaultPrefixes()
                        .withDefaultNamespace(CoreModule.of(version).getNamespaceURI())
                        .write(cityModel);
                logger.info("Exported CityGML v3.0 file to {}", exportFilePath);
            }
        } catch (CityGMLWriteException | CityGMLContextException e) {
            throw new RuntimeException(e);
        }
    }
}

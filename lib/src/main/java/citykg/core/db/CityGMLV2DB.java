package citykg.core.db;

import citykg.core.config.CityKGDBConfig;
import citykg.core.factory.AuxNodeLabels;
import citykg.core.factory.AuxPropNames;
import citykg.core.factory.EdgeTypes;
import citykg.core.ref.Neo4jRef;
import citykg.utils.BatchUtils;
import com.github.davidmoten.rtree.geometry.Geometries;
import org.apache.commons.numbers.core.Precision;
import org.citygml4j.CityGMLContext;
import org.citygml4j.builder.jaxb.CityGMLBuilder;
import org.citygml4j.builder.jaxb.CityGMLBuilderException;
import org.citygml4j.core.model.CityGMLVersion;
import org.citygml4j.model.citygml.CityGML;
import org.citygml4j.model.citygml.core.*;
import org.citygml4j.model.gml.base.AbstractGML;
import org.citygml4j.model.gml.base.AssociationByRepOrRef;
import org.citygml4j.model.gml.base.StringOrRef;
import org.citygml4j.model.gml.feature.BoundingShape;
import org.citygml4j.model.gml.geometry.primitives.*;
import org.citygml4j.model.module.ModuleContext;
import org.citygml4j.model.module.citygml.CityGMLModuleType;
import org.citygml4j.util.bbox.BoundingBoxOptions;
import org.citygml4j.util.gmlid.DefaultGMLIdManager;
import org.citygml4j.xml.io.CityGMLInputFactory;
import org.citygml4j.xml.io.CityGMLOutputFactory;
import org.citygml4j.xml.io.reader.*;
import org.citygml4j.xml.io.writer.CityGMLWriteException;
import org.citygml4j.xml.io.writer.CityGMLWriter;
import org.citygml4j.xml.io.writer.FeatureWriteMode;
import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.StreamSupport;

public class CityGMLV2DB extends CityKGDB {
    private final static Logger logger = LoggerFactory.getLogger(CityGMLV2DB.class);

    public CityGMLV2DB(String configPath) {
        this(new CityKGDBConfig(configPath));
    }

    public CityGMLV2DB(CityKGDBConfig config) {
        super(config);
        uuidClasses = Set.of(CityGML.class);
        idClasses = Set.of(AbstractGML.class);
        hrefClasses = Set.of(AssociationByRepOrRef.class, StringOrRef.class);
    }

    @Override
    protected Neo4jRef mapCityFile(String filePath, int partitionIndex, boolean connectToRoot) {
        final Neo4jRef[] cityModelRef = {null};
        try {
            CityKGDBConfig cityGMLConfig = (CityKGDBConfig) config;
            if (!cityGMLConfig.CITYGML_VERSION.equals("v2_0")) {
                logger.warn("Found CityGML version {}, expected version {}",
                        cityGMLConfig.CITYGML_VERSION, "v2_0");
            }
            dbStats.startTimer();
            CityGMLContext ctx = CityGMLContext.getInstance();
            CityGMLBuilder builder = ctx.createCityGMLBuilder();
            CityGMLInputFactory in = builder.createCityGMLInputFactory();
            in.setProperty(CityGMLInputFactory.FEATURE_READ_MODE, FeatureReadMode.SPLIT_PER_COLLECTION_MEMBER);
            CityGMLReader reader = in.createCityGMLReader(new File(filePath));
            logger.info("Reading CityGML v2.0 file {} chunk-wise into main memory", filePath);

            // Ids of top-level features with no existing bounding shapes
            List<Neo4jRef> topLevelNoBbox = Collections.synchronizedList(new ArrayList<>());

            // Multi-threading
            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            long tlCount = 0;
            while (reader.hasNext()) {
                final XMLChunk chunk = reader.nextChunk();
                tlCount++;

                long finalTlCount = tlCount;
                executorService.execute(() -> {
                    try {
                        CityGML object = chunk.unmarshal();
                        boolean toUpdateBboxTL = preProcessMapping(object);
                        Neo4jRef graphRef = (Neo4jRef) this.map(object,
                                AuxNodeLabels.__PARTITION_INDEX__.name() + partitionIndex);
                        postProcessMapping(toUpdateBboxTL, object, graphRef, partitionIndex, topLevelNoBbox);
                        logger.info("Mapped {} top-level features", finalTlCount);

                        if (object instanceof CityModel) {
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
                    } catch (UnmarshalException | MissingADESchemaException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            Neo4jDB.finishThreads(executorService, config.MAPPER_CONCURRENT_TIMEOUT);
            reader.close();
            logger.info("Mapped all {} top-level features", tlCount);
            logger.info("Finished mapping file {}", filePath);
            dbStats.stopTimer("Map input file [" + partitionIndex + "]");

            dbStats.startTimer();
            setIndexesIfNew();
            resolveXLinks(resolveLinkRules(), correctLinkRules(), partitionIndex);
            dbStats.stopTimer("Resolve links of input file [" + partitionIndex + "]");

            dbStats.startTimer();
            logger.info("Calculate and map bounding boxes of top-level features");
            calcTLBbox(topLevelNoBbox, partitionIndex);
            dbStats.stopTimer("Calculate and map bounding boxes of top-level features");

            logger.info("Finished mapping file {}", filePath);
        } catch (CityGMLBuilderException | CityGMLReadException e) {
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
                || aco.getLodRepresentation().hasLodImplicitGeometries()
                || aco.getLodRepresentation().hasImplicitGeometries();
    }

    @Override
    protected void calcTLBbox(List<Neo4jRef> topLevelNoBbox, int partitionIndex) {
        if (topLevelNoBbox == null) return;
        // Multithreaded
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        BatchUtils.toBatches(topLevelNoBbox, 10 * config.MAPPER_TOPLEVEL_BATCH_SIZE)
                .forEach(batch -> executorService.submit((Callable<Void>) () -> {
                    try (Transaction tx = graphDb.beginTx()) {
                        batch.forEach(graphRef -> {
                            // Calculate bounding shape
                            Node topLevelNode = graphRef.getRepresentationNode(tx);
                            AbstractCityObject aco = (AbstractCityObject) toObject(topLevelNode, null);
                            BoundingShape boundingShape = aco.calcBoundedBy(BoundingBoxOptions.defaults().assignResultToFeatures(true));
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
                    } catch (Exception e) {
                        throw new RuntimeException(e);
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

    protected Node getTopLevelListNode(Node cityModelNode) {
        return cityModelNode
                .getSingleRelationship(EdgeTypes.cityObjectMember, Direction.OUTGOING).getEndNode()
                .getSingleRelationship(EdgeTypes.elementData, Direction.OUTGOING).getEndNode();
    }

    @Override
    protected boolean isParentOfTopLevel(Node parentTopLevelNode) {
        return parentTopLevelNode.hasLabel(Label.label(CityObjectMember.class.getName()))
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
    protected boolean isTopLevel(Object obj) {
        return obj instanceof AbstractCityObject;
    }

    @Override
    public BiConsumer<Node, Object> handleOriginXLink() {
        return (node, obj) -> {
            // Only accept nodes whose "object" has more than one incoming "object" edge
            if (node.getSingleRelationship(EdgeTypes.object, Direction.OUTGOING).getEndNode()
                    .getRelationships(Direction.INCOMING, EdgeTypes.object)
                    .stream().count() <= 1) return;
            if (obj instanceof AssociationByRepOrRef<?> xlink) {
                if (xlink.getObject() instanceof AbstractGML child) {
                    xlink.setHref("#" + child.getId());
                    xlink.unsetObject();
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
        CityKGDBConfig cityKGDBConfig = (CityKGDBConfig) config;
        exportCityFile(cityKGDBConfig.CITYGML_EXPORT_PARTITION, cityKGDBConfig.CITYGML_EXPORT_BBOX);
    }

    @Override
    protected void exportCityFile(int partitionIndex, double[] topLevelBbox) {
        try (Transaction tx = graphDb.beginTx()) {
            // Get the CityModel node
            Node cityModelNode = tx.findNodes(Label.label(CityModel.class.getName()))
                    .stream()
                    .filter(node -> node.hasLabel(
                            Label.label(AuxNodeLabels.__PARTITION_INDEX__.toString() + partitionIndex)))
                    .findFirst()
                    .get();

            CityGMLContext ctx = CityGMLContext.getInstance();
            CityGMLBuilder builder = ctx.createCityGMLBuilder();
            CityGMLInputFactory in = builder.createCityGMLInputFactory();
            //in.parseSchema(new File("datasets/schemas/CityGML-NoiseADE-2_0_0.xsd"));

            CityGMLOutputFactory out = builder.createCityGMLOutputFactory(in.getSchemaHandler());
            ModuleContext moduleContext = new ModuleContext(); // v2.0.0
            FeatureWriteMode writeMode = FeatureWriteMode.NO_SPLIT; // SPLIT_PER_COLLECTION_MEMBER;

            // set to true and check the differences
            boolean splitOnCopy = true;

            out.setModuleContext(moduleContext);
            out.setGMLIdManager(DefaultGMLIdManager.getInstance());
            out.setProperty(CityGMLOutputFactory.FEATURE_WRITE_MODE, writeMode);
            out.setProperty(CityGMLOutputFactory.SPLIT_COPY, splitOnCopy);

            //out.setProperty(CityGMLOutputFactory.EXCLUDE_FROM_SPLITTING, ADEComponent.class);

            String exportFilePath = ((CityKGDBConfig) config).CITYGML_EXPORT_PATH;
            CityGMLWriter writer = out.createCityGMLWriter(new File(exportFilePath), "utf-8");
            writer.setPrefixes(moduleContext);
            writer.setDefaultNamespace(moduleContext.getModule(CityGMLModuleType.CORE));
            writer.setIndentString("  ");
            writer.setHeaderComment("written by citygml4j",
                    "using a CityGMLWriter instance",
                    "Split mode: " + writeMode,
                    "Split on copy: " + splitOnCopy);

            CityModel cityModel = (CityModel) toObject(cityModelNode, handleOriginXLink(), topLevelBbox, this::isParentOfTopLevel);
            writer.write(cityModel);
            writer.close();
            logger.info("CityGML file written to {}", exportFilePath);
            tx.commit();
        } catch (CityGMLBuilderException | CityGMLWriteException e) {
            throw new RuntimeException(e);
        }
    }

    // Match two bounding boxes in 3D
    private boolean matchBbox(
            double[] bbox1,
            double[] bbox2,
            Precision.DoubleEquivalence toleranceLength, // when comparing size length with 0
            double percentVolPass // matched only if overlapping volume over vol 1 and vol 2 is greater than this
    ) {
        if (bbox1 == null || bbox2 == null) return false;
        if (bbox1.length != 6 || bbox2.length != 6) return false;

        // Volume 1
        double vol1 = 0;
        double sizeX1 = bbox1[3] - bbox1[0];
        double sizeY1 = bbox1[4] - bbox1[1];
        double sizeZ1 = bbox1[5] - bbox1[2];
        if (toleranceLength.eq(sizeX1, 0)) vol1 = sizeY1 * sizeZ1;
        else if (toleranceLength.eq(sizeY1, 0)) vol1 = sizeX1 * sizeZ1;
        else if (toleranceLength.eq(sizeZ1, 0)) vol1 = sizeX1 * sizeY1;
        else vol1 = sizeX1 * sizeY1 * sizeZ1;

        // Volume 2
        double vol2 = 0;
        double sizeX2 = bbox2[3] - bbox2[0];
        double sizeY2 = bbox2[4] - bbox2[1];
        double sizeZ2 = bbox2[5] - bbox2[2];
        if (toleranceLength.eq(sizeX2, 0)) vol2 = sizeY2 * sizeZ2;
        else if (toleranceLength.eq(sizeY2, 0)) vol2 = sizeX2 * sizeZ2;
        else if (toleranceLength.eq(sizeZ2, 0)) vol2 = sizeX2 * sizeY2;
        else vol2 = sizeX2 * sizeY2 * sizeZ2;

        // Overlapping volume
        double overlap = 0;
        double x_overlap = Math.max(0, Math.min(bbox1[3], bbox2[3]) - Math.max(bbox1[0], bbox2[0]));
        double y_overlap = Math.max(0, Math.min(bbox1[4], bbox2[4]) - Math.max(bbox1[1], bbox2[1]));
        double z_overlap = Math.max(0, Math.min(bbox1[5], bbox2[5]) - Math.max(bbox1[2], bbox2[2]));
        if (toleranceLength.eq(x_overlap, 0)) overlap = y_overlap * z_overlap;
        else if (toleranceLength.eq(y_overlap, 0)) overlap = x_overlap * z_overlap;
        else if (toleranceLength.eq(z_overlap, 0)) overlap = x_overlap * y_overlap;
        else overlap = x_overlap * y_overlap * z_overlap;

        // Evaluate
        return overlap / vol1 > percentVolPass && overlap / vol2 > percentVolPass;
    }
}

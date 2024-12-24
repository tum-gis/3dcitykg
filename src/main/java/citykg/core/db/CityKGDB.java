package citykg.core.db;


import citykg.core.factory.*;
import citykg.core.ref.Neo4jRef;
import citykg.core.config.CityKGDBConfig;
import citykg.utils.ClazzUtils;
import citykg.utils.GraphUtils;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometry;
import org.apache.commons.lang3.function.TriConsumer;
import org.citygml4j.CityGMLContext;
import org.citygml4j.builder.jaxb.CityGMLBuilder;
import org.citygml4j.builder.jaxb.CityGMLBuilderException;
import org.citygml4j.core.model.CityGMLVersion;
import org.citygml4j.model.citygml.CityGML;
import org.citygml4j.model.citygml.core.CityModel;
import org.citygml4j.xml.io.CityGMLInputFactory;
import org.citygml4j.xml.io.reader.CityGMLReadException;
import org.citygml4j.xml.io.reader.CityGMLReader;
import org.citygml4j.xml.io.reader.FeatureReadMode;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class CityKGDB extends Neo4jDB {
    protected Neo4jRef ROOT_RTREES;
    protected RTree<Neo4jRef, Geometry>[] rtrees;
    protected static Set<Class<?>> uuidClasses;
    protected static Set<Class<?>> idClasses;
    protected static Set<Class<?>> hrefClasses;
    protected final static String hrefPrefix = "#";
    private final static Logger logger = LoggerFactory.getLogger(CityKGDB.class);

    @SuppressWarnings("unchecked")
    public CityKGDB(CityKGDBConfig config) {
        super(config);
        rtrees = new RTree[config.MAPPER_DATASET_PATHS.size()];
    }

    public void go() {
        int useCase = config.USE_CASE;
        switch (useCase) {
            case 0 -> {
                // Map only
                openEmpty();
                mapFromConfig();
                summarize();
            }
            case 1 -> {
                // Export only
                openExisting();
                exportCityFile();
            }
            case 2 -> {
                // Map and export
                openEmpty();
                mapFromConfig();
                summarize();
                exportCityFile();
            }
            default -> throw new RuntimeException("Invalid use case");
        }
        close();
    }

    protected abstract Neo4jRef mapCityFile(String filePath, int partitionIndex, boolean connectToRoot);

    protected abstract Class<?> getCityModelClass();

    protected abstract boolean toUpdateBboxTL(Object chunk);

    protected abstract void addToRtree(Object boundingShape, Neo4jRef graphRef, int partitionIndex);

    protected abstract void calcTLBbox(List<Neo4jRef> topLevelNoBbox, int partitionIndex);

    public abstract void exportCityFile();

    protected abstract void exportCityFile(int partitionIndex, double[] topLevelBbox);

    protected void setIndexesIfNew() {
        logger.info("|--> Updating indexes");
        mappedClassesTmp.stream()
                .filter(cl -> ClazzUtils.isSubclass(cl, uuidClasses))
                .forEach(cl -> setIndex(cl, AuxPropNames.__UUID__.toString()));
        mappedClassesTmp.stream()
                .filter(cl -> ClazzUtils.isSubclass(cl, idClasses))
                .forEach(cl -> setIndex(cl, PropNames.id.toString()));
        mappedClassesTmp.stream()
                .filter(cl -> ClazzUtils.isSubclass(cl, hrefClasses))
                .forEach(cl -> setIndex(cl, PropNames.href.toString()));
        waitForIndexes();
        // Store and clear mapped classes for next call
        mappedClassesSaved.addAll(mappedClassesTmp);
        mappedClassesTmp.clear();
        logger.info("-->| Updated indexes");
    }

    protected void connectCityModelToRoot(Neo4jRef cityModelRef, Map<String, Object> relationshipProperties) {
        try (Transaction tx = graphDb.beginTx()) {
            Node cityModel = cityModelRef.getRepresentationNode(tx);
            Node mapperRoot = ROOT_MAPPER.getRepresentationNode(tx);
            // root -[rel]-> createdNode
            Relationship rel = mapperRoot.createRelationshipTo(cityModel, AuxEdgeTypes.COLLECTION_MEMBER);
            for (Map.Entry<String, Object> entry : relationshipProperties.entrySet()) {
                rel.setProperty(entry.getKey(), entry.getValue());
            }
            tx.commit();
            logger.debug("Connected a city model to the database");
        } catch (Exception e) {
            logger.error(e.getMessage() + " (A)\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    public void mapFromConfig() {
        for (int i = 0; i < config.MAPPER_DATASET_PATHS.size(); i++) {
            // An RTree layer for each input dataset
            rtrees[i] = RTree.star().create();

            String stringPath = config.MAPPER_DATASET_PATHS.get(i);
            Path path = Path.of(stringPath);
            if (Files.isDirectory(path)) {
                // Consider all files from this directory as one single dataset
                logger.info("Input CityGML directory {} found", stringPath);
                mapCityDir(path, i);
            } else {
                // Is a file
                mapCityFile(stringPath, i, true);
            }
        }
        if (!config.NEO4J_RTREE_IMG_PATH.isBlank()) {
            // Export all RTree layers' footprint as images
            exportRTreeFootprints(config.NEO4J_RTREE_IMG_PATH);
        }
        logger.info("Finished mapping all files from config");
    }

    // Map small files from a directory -> each file is loaded into one thread
    // TODO Momentarily for CityGML v2.0 only
    protected void mapCityDir(Path path, int partitionIndex) {
        dbStats.startTimer();

        CityKGDBConfig cityGMLConfig = (CityKGDBConfig) config;
        if (cityGMLConfig.CITYGML_VERSION.equals("v2_0")) {
            logger.warn("Found CityGML version {}, expected version {}",
                    cityGMLConfig.CITYGML_VERSION, CityGMLVersion.v2_0);
        }

        // Multi-threading
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        // Set provides constant time for adds and removes of huge lists
        Set<Neo4jRef> cityModelRefs = Collections.synchronizedSet(new HashSet<>());
        // Ids of top-level features with no existing bounding shapes
        List<Neo4jRef> topLevelNoBbox = Collections.synchronizedList(new ArrayList<>());
        AtomicLong tlCountDir = new AtomicLong(0);
        try (Stream<Path> st = Files.walk(path)) {
            st.filter(Files::isRegularFile).forEach(file -> {
                // One file one thread
                executorService.submit((Callable<Void>) () -> {
                    try {
                        CityGMLContext ctx = CityGMLContext.getInstance();
                        CityGMLBuilder builder = ctx.createCityGMLBuilder();
                        CityGMLInputFactory in = builder.createCityGMLInputFactory();
                        in.setProperty(CityGMLInputFactory.FEATURE_READ_MODE, FeatureReadMode.SPLIT_PER_COLLECTION_MEMBER);
                        try (CityGMLReader reader = in.createCityGMLReader(file.toFile())) {
                            logger.info("Reading file {}", file);
                            long tlCountFile = 0;
                            while (reader.hasNext()) {
                                CityGML chunk = reader.nextFeature();
                                tlCountFile++;
                                boolean toUpdateBboxTL = preProcessMapping(chunk);
                                Neo4jRef graphRef = (Neo4jRef) this.map(chunk,
                                        AuxNodeLabels.__PARTITION_INDEX__.name() + partitionIndex);
                                postProcessMapping(toUpdateBboxTL, chunk, graphRef, partitionIndex, topLevelNoBbox);
                                logger.debug("Mapped {} top-level features", tlCountFile);

                                if (chunk instanceof CityModel) {
                                    cityModelRefs.add(graphRef);
                                }
                            }
                            tlCountDir.addAndGet(tlCountFile);
                        }
                    } catch (CityGMLBuilderException | CityGMLReadException e) {
                        throw new RuntimeException(e);
                    }

                    return null;
                });
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            Neo4jDB.finishThreads(executorService, config.MAPPER_CONCURRENT_TIMEOUT);
        }
        logger.info("Mapped {} top-level features from directory {}", tlCountDir, path.toString());
        dbStats.stopTimer("Map all input tiled files in " + path.toString());

        dbStats.startTimer();
        logger.info("Resolve links of tiled files in input directory {} {},", partitionIndex, path.toString());
        setIndexesIfNew();
        resolveXLinks(resolveLinkRules(), correctLinkRules(), partitionIndex);
        dbStats.stopTimer("Resolve links of tiled files in input directory " + path.toString());

        dbStats.startTimer();
        logger.info("Calculate and map bounding boxes of top-level features");
        calcTLBbox(topLevelNoBbox, partitionIndex);
        dbStats.stopTimer("Calculate and map bounding boxes of top-level features");

        // Merge all CityModel objects to one
        dbStats.startTimer();
        try (Transaction tx = graphDb.beginTx()) {
            Node mapperRoot = ROOT_MAPPER.getRepresentationNode(tx);
            Node mergedCityModelNode = null;
            for (Neo4jRef cityModelRef : cityModelRefs) {
                Node cityModelNode = cityModelRef.getRepresentationNode(tx);
                if (mergedCityModelNode == null) {
                    mergedCityModelNode = GraphUtils.clone(tx, cityModelNode, true);
                    // root -[rel]-> createdNode
                    Relationship rel = mapperRoot.createRelationshipTo(mergedCityModelNode, AuxEdgeTypes.COLLECTION_MEMBER);
                    rel.setProperty(AuxPropNames.COLLECTION_INDEX.toString(), partitionIndex);
                    rel.setProperty(AuxPropNames.COLLECTION_MEMBER_TYPE.toString(), getCityModelClass().getName());
                    logger.debug("Connected a city model to the database");
                    continue;
                }
                GraphUtils.cloneRelationships(tx, cityModelNode, mergedCityModelNode, true);
                Lock lockCityModelNode = tx.acquireWriteLock(cityModelNode);
                cityModelNode.delete();
                lockCityModelNode.release();
            }
            tx.commit();
        } catch (Exception e) {
            logger.error(e.getMessage() + " (B)\n" + Arrays.toString(e.getStackTrace()));
        }
        dbStats.stopTimer("Merge all CityModel objects to one [" + partitionIndex + "]");

        logger.info("Finished mapping directory {}", path.toString());
    }

    public void resolveXLinks(TriConsumer<Transaction, Node, Node> resolveLinkRules,
                              BiConsumer<Transaction, Node> correctLinkRules,
                              int partitionIndex) {
        logger.info("|--> Resolving XLinks");
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<String> nodeIds = Collections.synchronizedList(new ArrayList<>());

        // Store all node IDs in a list first
        mappedClassesSaved.stream()
                .filter(clazz -> ClazzUtils.isSubclass(clazz, hrefClasses))
                .forEach(hrefClass -> executorService.submit((Callable<Void>) () -> {
                    try (Transaction tx = graphDb.beginTx()) {
                        logger.info("Collecting node index {}", hrefClass.getName());
                        tx.findNodes(Label.label(hrefClass.getName())).stream()
                                .filter(hrefNode -> hrefNode.hasLabel(
                                        Label.label(AuxNodeLabels.__PARTITION_INDEX__.name() + partitionIndex)))
                                .filter(hrefNode -> hrefNode.hasProperty(PropNames.href.toString()))
                                .forEach(hrefNode -> {
                                    nodeIds.add(hrefNode.getElementId());
                                });
                        tx.commit();
                    } catch (Exception e) {
                        logger.error(e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
                    }
                    return null;
                }));
        Neo4jDB.finishThreads(executorService, config.MAPPER_CONCURRENT_TIMEOUT);

        // Batch transactions
        List<List<String>> batches = new ArrayList<>();
        int batchSize = config.DB_BATCH_SIZE;
        for (int i = 0; i < nodeIds.size(); i += batchSize) {
            batches.add(nodeIds.subList(i, Math.min(i + batchSize, nodeIds.size())));
        }
        logger.info("Initiated {} batches for resolving XLinks", batches.size());

        // Resolve XLinks
        AtomicInteger transactionCount = new AtomicInteger();
        ExecutorService esBatch = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        batches.forEach(batch -> esBatch.submit((Callable<Void>) () -> {
            try (Transaction tx = graphDb.beginTx()) {
                batch.forEach(nodeId -> {
                    Node hrefNode = tx.getNodeByElementId(nodeId);
                    correctLinkRules.accept(tx, hrefNode);
                    AtomicInteger idCount = new AtomicInteger();
                    String id = hrefNode.getProperty(PropNames.href.toString()).toString()
                            .replace("#", "");
                    mappedClassesSaved.stream()
                            .filter(clazz -> ClazzUtils.isSubclass(clazz, idClasses))
                            .forEach(idClass -> tx.findNodes(
                                    Label.label(idClass.getName()),
                                    PropNames.id.toString(),
                                    id
                            ).stream().filter(idNode -> idNode.hasLabel(
                                    Label.label(AuxNodeLabels.__PARTITION_INDEX__.name() + partitionIndex))
                            ).forEach(idNode -> {
                                // Connect linked node to source id node
                                resolveLinkRules.accept(tx, idNode, hrefNode);
                                idCount.getAndIncrement();
                                transactionCount.getAndIncrement();
                            }));
                    if (idCount.intValue() == 0) {
                        logger.warn("No element with referenced ID = {} found", id);
                    } else if (idCount.intValue() >= 2) {
                        logger.warn("{} elements of the same ID = {} detected", idCount, id);
                    }
                });
                if (transactionCount.get() > 0)
                    logger.info("Found and resolved {} XLink(s)", transactionCount);
                tx.commit();
            } catch (Exception e) {
                logger.error(e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
            }
            return null;
        }));
        Neo4jDB.finishThreads(esBatch, config.MAPPER_CONCURRENT_TIMEOUT);

        logger.info("-->| Resolved XLinks");
    }

    // Returns true if the given node is reachable from the CityModel node assigned with a partition index
    // Use this AFTER AT LEAST one city model has been fully mapped and its links resolved
    protected Function<Node, Boolean> idFilterRules() {
        return (Node idNode) -> GraphUtils.isReachable(getCityModelClass().getName(), idNode);
    }

    // Define rules how to link an ID and referenced node (such as href)
    protected TriConsumer<Transaction, Node, Node> resolveLinkRules() {
        return (Transaction tx, Node idNode, Node linkedNode) -> {
            // (:CityModel) -[:cityObjectMember]-> (:COLLECTION)
            //              -[:COLLECTION_MEMBER]-> (:CityObjectMember.href)
            //              -[:object]-> (:Feature.id)
            // (:Building)  -[:lod2Solid]-> (:SolidProperty)
            //              -[:object]-> (:Solid)
            //              -[:exterior]-> (:SurfaceProperty)
            //              -[:object]-> (:CompositeSurface)
            //              -[:surfaceMember]-> (:COLLECTION)
            //              -[:COLLECTION_MEMBER]-> (:SurfaceProperty)
            //              -[:object]-> (:Polygon)
            Lock lockLinkedNode = tx.acquireWriteLock(linkedNode);
            Lock lockIdNode = tx.acquireWriteLock(idNode);
            linkedNode.createRelationshipTo(idNode, EdgeTypes.object);
            linkedNode.removeProperty(PropNames.href.toString());
            lockLinkedNode.release();
            lockIdNode.release();
        };
    }

    protected BiConsumer<Transaction, Node> correctLinkRules() {
        return (Transaction tx, Node node) -> {
            // Correct hrefs without prefix "#"
            Object propertyHref = node.getProperty(PropNames.href.toString());
            if (propertyHref == null) return;
            String valueHref = node.getProperty(PropNames.href.toString()).toString();
            if (valueHref.isBlank()) {
                logger.warn("Ignored empty href");
                return;
            }
            if (valueHref.charAt(0) != '#') {
                logger.warn("Added prefix \"#\" to incomplete href = {}", valueHref);
                Lock lock = tx.acquireWriteLock(node);
                node.setProperty(PropNames.href.toString(), "#" + valueHref);
                lock.release();
            }

            // More corrections when needed...
        };
    }

    protected abstract Node getTopLevelListNode(Node cityModelNode);

    protected abstract boolean isParentOfTopLevel(Node parentTopLevelNode);

    protected abstract boolean isTopLevel(Node node);

    protected abstract boolean isTopLevel(Object obj);

    // protected abstract Node getAnchorNode(Transaction tx, Node node, Label anchor);

    private Node getAnchorNode(Transaction tx, Node node, Label anchor) {
        Traverser traverser = tx.traversalDescription()
                .depthFirst()
                .expand(PathExpanders.forDirection(Direction.OUTGOING))
                .evaluator(Evaluators.fromDepth(0))
                .evaluator(path -> {
                    if (path.endNode().hasLabel(anchor))
                        return Evaluation.INCLUDE_AND_PRUNE;
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                })
                .traverse(node);
        List<Node> anchorNodes = new ArrayList<>();
        traverser.forEach(path -> anchorNodes.add(path.endNode()));
        if (anchorNodes.isEmpty()) {
            logger.error("Found no anchor node {}, attaching to source node, where change occurred", anchor.name());
            return node;
        }
        if (anchorNodes.size() > 1) {
            logger.error("Found more than one anchor node {}, selecting one", anchor.name());
        }
        return anchorNodes.get(0);
    }

    protected abstract boolean preProcessMapping(Object chunk);

    protected abstract void postProcessMapping(boolean toUpdateBboxTL, Object chunk, Neo4jRef graphRef, int partitionIndex, List<Neo4jRef> topLevelNoBbox);

    public abstract BiConsumer<Node, Object> handleOriginXLink();

    public void exportRTreeFootprints(String folderPath) {
        try {
            for (int i = 0; i < rtrees.length; i++) {
                Path filePath = Path.of(folderPath).resolve("rtree_" + i + ".png");
                Files.deleteIfExists(filePath);
                File file = Files.createFile(filePath).toFile();
                rtrees[i].visualize(1000, 1000).save(file.getAbsoluteFile().toString());
            }
            logger.info("Exported RTree footprint(s) to folder {}", folderPath);
        } catch (IOException e) {
            throw new RuntimeException("Could not export RTree footprints " + e);
        }
    }

    public void storeRTrees() {
        try (Transaction tx = graphDb.beginTx()) {
            logger.info("|--> Storing RTrees in the database");
            ROOT_RTREES = (Neo4jRef) map(rtrees);
            Node rootRtreesNode = ROOT_RTREES.getRepresentationNode(tx);
            rootRtreesNode.addLabel(NodeLabels.__ROOT_RTREES__);
            Node rootNode = ROOT.getRepresentationNode(tx);
            rootNode.createRelationshipTo(rootRtreesNode, AuxEdgeTypes.RTREES);
            // TODO Set explicit edges to connect RTrees and CityGML nodes?
            tx.commit();
            logger.info("-->| Stored RTrees in the database");
        } catch (Exception e) {
            logger.error(e.getMessage() + " " + Arrays.toString(e.getStackTrace()));
        }
    }

    @Override
    public void summarize() {
        super.summarize();

        if (config.NEO4J_RTREE_STORE) {
            // Store all RTree layers in the database for later use/import
            storeRTrees();
        }
    }

    public RTree<Neo4jRef, Geometry>[] getRtrees() {
        return rtrees;
    }

    public void setRtrees(RTree<Neo4jRef, Geometry>[] rtrees) {
        this.rtrees = rtrees;
    }
}

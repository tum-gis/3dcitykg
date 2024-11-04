package citykg.core.db.citygml;

import citykg.core.config.CityKGDBConfig;
import citykg.core.db.CityKGDB;
import citykg.core.db.Neo4jDB;
import citykg.core.factory.AuxNodeLabels;
import citykg.core.factory.AuxPropNames;
import citykg.core.factory.EdgeTypes;
import citykg.core.ref.Neo4jRef;
import citykg.utils.BatchUtils;
import citykg.utils.ClazzUtils;
import citykg.utils.MetricBoundarySurfaceProperty;
import com.github.davidmoten.rtree.geometry.Geometries;
import org.apache.commons.geometry.euclidean.threed.*;
import org.apache.commons.geometry.euclidean.threed.line.Line3D;
import org.apache.commons.geometry.euclidean.threed.line.Lines3D;
import org.apache.commons.numbers.core.Precision;
import org.citygml4j.CityGMLContext;
import org.citygml4j.builder.jaxb.CityGMLBuilder;
import org.citygml4j.builder.jaxb.CityGMLBuilderException;
import org.citygml4j.core.model.CityGMLVersion;
import org.citygml4j.model.citygml.CityGML;
import org.citygml4j.model.citygml.bridge.BridgePartProperty;
import org.citygml4j.model.citygml.building.*;
import org.citygml4j.model.citygml.core.*;
import org.citygml4j.model.citygml.tunnel.TunnelPartProperty;
import org.citygml4j.model.gml.base.AbstractGML;
import org.citygml4j.model.gml.base.AssociationByRepOrRef;
import org.citygml4j.model.gml.base.StringOrRef;
import org.citygml4j.model.gml.feature.BoundingShape;
import org.citygml4j.model.gml.feature.FeatureProperty;
import org.citygml4j.model.gml.geometry.aggregates.MultiCurve;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CityGMLDBV2 extends CityKGDB {
    private final static Logger logger = LoggerFactory.getLogger(CityGMLDBV2.class);

    public CityGMLDBV2(CityKGDBConfig config) {
        super(config);
        uuidClasses = Set.of(CityGML.class);
        idClasses = Set.of(AbstractGML.class);
        hrefClasses = Set.of(AssociationByRepOrRef.class, StringOrRef.class);
    }

    @Override
    protected Neo4jRef mapFileCityGML(String filePath, int partitionIndex, boolean connectToRoot) {
        final Neo4jRef[] cityModelRef = {null};
        try {
            CityKGDBConfig cityGMLConfig = (CityKGDBConfig) config;
            if (cityGMLConfig.CITYGML_VERSION != CityGMLVersion.v2_0) {
                logger.warn("Found CityGML version {}, expected version {}",
                        cityGMLConfig.CITYGML_VERSION, CityGMLVersion.v2_0);
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
    protected boolean isCOMTopLevel(Node cityObjectMemberNode) {
        return isTopLevel(
                cityObjectMemberNode.getSingleRelationship(EdgeTypes.object, Direction.OUTGOING).getEndNode());
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
    protected String getCOMElementId(Transaction tx, Neo4jRef topLevelRef) {
        return topLevelRef.getRepresentationNode(tx)
                .getRelationships(Direction.INCOMING, EdgeTypes.object).stream()
                // There maybe multiple incoming relationships "object" (due to CityObjectMember and CityObjectGroup)
                // -> choose the one with CityObjectMember
                // TODO Also consider CityObjectGroup?
                .filter(rel -> rel.getStartNode().hasLabel(Label.label(CityObjectMember.class.getName())))
                .findFirst().get().getStartNode().getElementId();
    }

    @Override
    protected List<Label> skipLabelsForTopLevel() {
        return List.of(Label.label(Solid.class.getName()));
    }

    /*
    @Override
    protected Node getAnchorNode(Transaction tx, Node node, Label anchor) {
        // BoundarySurfaceProperty -[*]-> SurfaceProperty
        return node.getSingleRelationship(EdgeTypes.object, Direction.OUTGOING).getEndNode()
                .getSingleRelationship(EdgeTypes.lod2MultiSurface, Direction.OUTGOING).getEndNode()
                .getSingleRelationship(EdgeTypes.object, Direction.OUTGOING).getEndNode()
                .getSingleRelationship(EdgeTypes.surfaceMember, Direction.OUTGOING).getEndNode()
                .getSingleRelationship(EdgeTypes.elementData, Direction.OUTGOING).getEndNode()
                .getSingleRelationship(AuxEdgeTypes.ARRAY_MEMBER, Direction.OUTGOING).getEndNode();
    }
    */

    protected boolean isPartProperty(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(), false).anyMatch(l -> {
            try {
                return ClazzUtils.isSubclass(Class.forName(l.name()), FeatureProperty.class, BuildingPartProperty.class)
                        || ClazzUtils.isSubclass(Class.forName(l.name()), FeatureProperty.class, BridgePartProperty.class)
                        || ClazzUtils.isSubclass(Class.forName(l.name()), FeatureProperty.class, TunnelPartProperty.class);
            } catch (ClassNotFoundException e) {
                return false;
            }
        });
    }

    protected boolean isBoundarySurfaceProperty(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(), false).anyMatch(l -> {
            try {
                return ClazzUtils.isSubclass(Class.forName(l.name()), FeatureProperty.class, AbstractBoundarySurface.class)
                        || ClazzUtils.isSubclass(Class.forName(l.name()), FeatureProperty.class, org.citygml4j.model.citygml.bridge.AbstractBoundarySurface.class)
                        || ClazzUtils.isSubclass(Class.forName(l.name()), FeatureProperty.class, org.citygml4j.model.citygml.tunnel.AbstractBoundarySurface.class);
            } catch (ClassNotFoundException e) {
                return false;
            }
        });
    }

    @Override
    protected ConvexPolygon3D toConvexPolygon3D(Object polygon, Precision.DoubleEquivalence precision) {
        if (!(polygon instanceof Polygon poly)) return null;
        if (!poly.isSetExterior()) return null;

        List<Vector3D> vectorList = new ArrayList<>(); // path must be closed (last = first)
        List<Double> points = poly.getExterior().getRing().toList3d();

        for (int i = 0; i < points.size(); i += 3) {
            //double x = Double.parseDouble(String.valueOf(ps.get(i)));
            //double y = Double.parseDouble(String.valueOf(ps.get(i + 1)));
            //double z = Double.parseDouble(String.valueOf(ps.get(i + 2)));
            double x = points.get(i);
            double y = points.get(i + 1);
            double z = points.get(i + 2);
            vectorList.add(Vector3D.of(x, y, z));
        }

        ConvexPolygon3D result = null;
        try {
            result = Planes.convexPolygonFromVertices(vectorList, precision);
        } catch (IllegalArgumentException e) {
            logger.warn(e.getMessage());
        }

        return result;
    }

    @Override
    protected double[] multiCurveBBox(Object multiCurve) {
        if (!(multiCurve instanceof MultiCurve mc)) return null;
        List<Double> points = new ArrayList<>();
        for (CurveProperty cp : mc.getCurveMember()) { // TODO getCurveMembers()?
            LineString ls = (LineString) cp.getCurve(); // TODO Other AbstractCurve types than LineString?
            List<Double> tmpPoints = ls.toList3d();
            points.addAll(tmpPoints);
        }
        double[] bbox = new double[6];
        double minX = Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double minZ = Double.MAX_VALUE;
        double maxX = Double.MIN_VALUE;
        double maxY = Double.MIN_VALUE;
        double maxZ = Double.MIN_VALUE;
        for (int i = 0; i < points.size(); i += 3) {
            double vX = points.get(i);
            double vY = points.get(i + 1);
            double vZ = points.get(i + 2);
            if (vX < minX) {
                minX = vX;
            } else if (vX > maxX) {
                maxX = vX;
            }
            if (vY < minY) {
                minY = vY;
            } else if (vY > maxY) {
                maxY = vY;
            }
            if (vZ < minZ) {
                minZ = vZ;
            } else if (vZ > maxZ) {
                maxZ = vZ;
            }
        }
        bbox[0] = minX;
        bbox[1] = minY;
        bbox[2] = minZ;
        bbox[3] = maxX;
        bbox[4] = maxY;
        bbox[5] = maxZ;
        return bbox;
    }

    @Override
    // Create a list of lines containing non-collinear points given in a multiCurve
    protected List<Line3D> multiCurveToLines3D(Object multiCurve, Precision.DoubleEquivalence precision) {
        if (!(multiCurve instanceof MultiCurve mc)) return null;
        List<Line3D> lines = new ArrayList<>();
        for (CurveProperty cp : mc.getCurveMember()) { // TODO getCurveMembers()?
            LineString ls = (LineString) cp.getCurve(); // TODO Other AbstractCurve types than LineString?
            List<Double> points = ls.toList3d();
            if (points.size() < 6) {
                logger.warn("LineString contains too few points");
                continue;
            }
            // First line from first two points
            Vector3D point1 = Vector3D.of(points.get(0), points.get(1), points.get(2));
            Vector3D point2 = Vector3D.of(points.get(3), points.get(4), points.get(5));
            int iPoint = 3;
            while (point1.eq(point2, precision) && iPoint <= points.size() - 3) {
                iPoint += 3;
                point2 = Vector3D.of(points.get(iPoint), points.get(iPoint + 1), points.get(iPoint + 2));
            }
            if (point1.eq(point2, precision)) {
                logger.warn("LineString contains only collinear points, ignoring");
                continue;
            }
            lines.add(Lines3D.fromPoints(point1, point2, precision));

            int iSaved = 3;
            for (int i = 6; i < points.size(); i += 3) {
                Vector3D point = Vector3D.of(points.get(i), points.get(i + 1), points.get(i + 2));
                Line3D lastLine = lines.get(lines.size() - 1);
                if (lastLine.contains(point)) continue; // Collinear point to the last line
                lines.add(Lines3D.fromPoints(
                        Vector3D.of(points.get(iSaved), points.get(iSaved + 1), points.get(iSaved + 2)),
                        point, precision));
                iSaved = i;
            }
        }
        return lines;
    }

    @Override
    protected boolean isMultiCurveContainedInLines3D(Object multiCurve, List<Line3D> lines, Precision.DoubleEquivalence precision) {
        if (!(multiCurve instanceof MultiCurve mc)) return false;
        for (CurveProperty cp : mc.getCurveMember()) { // TODO getCurveMembers()?
            LineString ls = (LineString) cp.getCurve(); // TODO Other AbstractCurve types than LineString?
            List<Double> points = ls.toList3d();
            for (int i = 0; i < points.size(); i += 3) {
                boolean contained = false;
                for (Line3D line : lines) {
                    Vector3D point = Vector3D.of(points.get(i), points.get(i + 1), points.get(i + 2));
                    if (line.contains(point)) {
                        contained = true;
                        break;
                    }
                }
                if (!contained) return false;
            }
        }
        return true;
    }

    @Override
    protected MetricBoundarySurfaceProperty metricFromBoundarySurfaceProperty(
            Node node,
            Precision.DoubleEquivalence lengthPrecision,
            Precision.DoubleEquivalence anglePrecision
    ) {
        // Check types
        List<SurfaceProperty> sps;
        Class<?> surfaceType;
        int highestLOD;
        if (ClazzUtils.isInstanceOf(node, BoundarySurfaceProperty.class)) {
            BoundarySurfaceProperty bsp = (BoundarySurfaceProperty) toObject(node, null);
            AbstractBoundarySurface bs = bsp.getBoundarySurface();
            surfaceType = bs.getClass();
            if (bs.isSetLod4MultiSurface()) {
                sps = bs.getLod4MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 4;
            } else if (bs.isSetLod3MultiSurface()) {
                sps = bs.getLod3MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 3;
            } else if (bs.isSetLod2MultiSurface()) {
                sps = bs.getLod2MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 2;
            } else {
                logger.error("Building.BoundarySurfaceProperty, node id = {}, has no MultiSurface, ignoring", node.getElementId());
                return null;
            }
        } else if (ClazzUtils.isInstanceOf(node, org.citygml4j.model.citygml.bridge.BoundarySurfaceProperty.class)) {
            org.citygml4j.model.citygml.bridge.BoundarySurfaceProperty bsp = (org.citygml4j.model.citygml.bridge.BoundarySurfaceProperty) toObject(node, null);
            org.citygml4j.model.citygml.bridge.AbstractBoundarySurface bs = bsp.getBoundarySurface();
            surfaceType = bs.getClass();
            if (bs.isSetLod4MultiSurface()) {
                sps = bs.getLod4MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 4;
            } else if (bs.isSetLod3MultiSurface()) {
                sps = bs.getLod3MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 3;
            } else if (bs.isSetLod2MultiSurface()) {
                sps = bs.getLod2MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 2;
            } else {
                logger.error("Bridge.BoundarySurfaceProperty, node id = {}, has no MultiSurface, ignoring", node.getElementId());
                return null;
            }
        } else if (ClazzUtils.isInstanceOf(node, org.citygml4j.model.citygml.tunnel.BoundarySurfaceProperty.class)) {
            org.citygml4j.model.citygml.tunnel.BoundarySurfaceProperty bsp = (org.citygml4j.model.citygml.tunnel.BoundarySurfaceProperty) toObject(node, null);
            org.citygml4j.model.citygml.tunnel.AbstractBoundarySurface bs = bsp.getBoundarySurface();
            surfaceType = bs.getClass();
            if (bs.isSetLod4MultiSurface()) {
                sps = bs.getLod4MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 4;
            } else if (bs.isSetLod3MultiSurface()) {
                sps = bs.getLod3MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 3;
            } else if (bs.isSetLod2MultiSurface()) {
                sps = bs.getLod2MultiSurface().getMultiSurface().getSurfaceMember();
                highestLOD = 2;
            } else {
                logger.error("Tunnel.BoundarySurfaceProperty, node id = {}, has no MultiSurface, ignoring", node.getElementId());
                return null;
            }
        } else {
            logger.error("Node id = {} is not a BoundarySurfaceProperty, ignoring", node.getElementId());
            return null;
        }

        // Calculate bbox and normal vector for each surface (using its exterior)
        List<Vector3D> normals = new ArrayList<>();
        List<Vector3D[]> bboxes = new ArrayList<>();
        List<Vector3D> points = new ArrayList<>();
        for (SurfaceProperty sp : sps) {
            Polygon poly = (Polygon) sp.getSurface(); // TODO Other types than Polygon?
            List<Double> surfacePoints = poly.getExterior().getRing().toList3d();
            List<Vector3D> surfaceVectors = new ArrayList<>();
            for (int i = 0; i < surfacePoints.size(); i += 3) {
                Vector3D v = Vector3D.of(surfacePoints.get(i), surfacePoints.get(i + 1), surfacePoints.get(i + 2));
                surfaceVectors.add(v);
            }
            points.addAll(surfaceVectors);

            // Surface normal (normalized, with uniform direction -> independent of order of points)
            Vector3D surfaceNormal = Planes.fromPoints(surfaceVectors, lengthPrecision).getNormal().normalize();
            /*if (sps.size() > 1) {
                // Only bring the normal vector to the same side of the plane in case of multiple surfaces
                if (anglePrecision.lt(surfaceNormal.angle(Vector3D.of(1, 1, 1)), 0)) {
                    surfaceNormal = surfaceNormal.negate();
                }
            }*/
            normals.add(surfaceNormal);

            // Bounding box
            Vector3D[] bbox = {Vector3D.min(surfaceVectors), Vector3D.max(surfaceVectors)};
            bboxes.add(bbox);
        }

        // Calculate the normal vector of all member surfaces
        Vector3D normal = Vector3D.ZERO;
        for (Vector3D n : normals) {
            normal = normal.add(n);
        }
        normal = normal.normalize();

        // Bounding box overall
        Vector3D[] bbox = {
                Vector3D.min(bboxes.stream().map(b -> b[0]).collect(Collectors.toList())),
                Vector3D.max(bboxes.stream().map(b -> b[1]).collect(Collectors.toList()))
        };

        return new MetricBoundarySurfaceProperty(surfaceType, normal, bbox, points, highestLOD);
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
    public void exportCityGML(int partitionIndex, String exportFilePath) {
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

            CityGMLWriter writer = out.createCityGMLWriter(new File(exportFilePath), "utf-8");
            writer.setPrefixes(moduleContext);
            writer.setDefaultNamespace(moduleContext.getModule(CityGMLModuleType.CORE));
            writer.setIndentString("  ");
            writer.setHeaderComment("written by citygml4j",
                    "using a CityGMLWriter instance",
                    "Split mode: " + writeMode,
                    "Split on copy: " + splitOnCopy);

            CityModel cityModel = (CityModel) toObject(cityModelNode, handleOriginXLink());
            writer.write(cityModel);
            writer.close();
            logger.info("CityGML file written to {}", exportFilePath);
            tx.commit();
        } catch (CityGMLBuilderException | CityGMLWriteException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void testImportAndExport(String importFilePath, String exportFilePath) {
        try (Transaction tx = graphDb.beginTx()) {
            // IMPORT

            Neo4jRef cityModelRef = mapFileCityGML(importFilePath, 0, false);
            Node cityModelNode = cityModelRef.getRepresentationNode(tx);

            // EXPORT

            CityGMLContext ctx = CityGMLContext.getInstance();
            CityGMLBuilder builder = ctx.createCityGMLBuilder();
            CityGMLInputFactory in = builder.createCityGMLInputFactory();
            //in.parseSchema(new File("datasets/schemas/CityGML-NoiseADE-2_0_0.xsd"));

            CityGMLOutputFactory out = builder.createCityGMLOutputFactory(in.getSchemaHandler());
            ModuleContext moduleContext = new ModuleContext(); // v2.0.0
            FeatureWriteMode writeMode = FeatureWriteMode.SPLIT_PER_COLLECTION_MEMBER; // SPLIT_PER_COLLECTION_MEMBER;

            // set to true and check the differences
            boolean splitOnCopy = false;

            out.setModuleContext(moduleContext);
            out.setGMLIdManager(DefaultGMLIdManager.getInstance());
            out.setProperty(CityGMLOutputFactory.FEATURE_WRITE_MODE, writeMode);
            out.setProperty(CityGMLOutputFactory.SPLIT_COPY, splitOnCopy);

            //out.setProperty(CityGMLOutputFactory.EXCLUDE_FROM_SPLITTING, ADEComponent.class);

            CityGMLWriter writer = out.createCityGMLWriter(new File(exportFilePath), "utf-8");
            writer.setPrefixes(moduleContext);
            writer.setDefaultNamespace(moduleContext.getModule(CityGMLModuleType.CORE));
            writer.setIndentString("  ");
            writer.setHeaderComment("written by citygml4j",
                    "using a CityGMLWriter instance",
                    "Split mode: " + writeMode,
                    "Split on copy: " + splitOnCopy);

            CityModel cityModel = (CityModel) toObject(cityModelNode, handleOriginXLink());
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

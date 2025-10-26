package citykg.core.db;


import citykg.core.config.CityKGDBConfig;
import citykg.core.factory.AuxNodeLabels;
import citykg.core.ref.Neo4jRef;
import org.citygml4j.cityjson.CityJSONContext;
import org.citygml4j.cityjson.CityJSONContextException;
import org.citygml4j.cityjson.model.CityJSONVersion;
import org.citygml4j.cityjson.model.metadata.Metadata;
import org.citygml4j.cityjson.reader.CityJSONInputFactory;
import org.citygml4j.cityjson.reader.CityJSONReadException;
import org.citygml4j.cityjson.reader.CityJSONReader;
import org.citygml4j.cityjson.writer.CityJSONOutputFactory;
import org.citygml4j.cityjson.writer.CityJSONWriteException;
import org.citygml4j.cityjson.writer.CityJSONWriter;
import org.citygml4j.core.model.core.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import org.slf4j.Logger;

// TODO Currently no R-Tree support for CityJSON
public class CityJSONDB extends Neo4jDB {
    private final static Logger logger = LoggerFactory.getLogger(CityJSONDB.class);

    public CityJSONDB(String configPath) {
        this(new CityKGDBConfig(configPath));
    }

    public CityJSONDB(CityKGDBConfig config) {
        super(config);
    }

    @Override
    public void go() {
        int useCase = config.USE_CASE;
        switch (useCase) {
            case 0 -> {
                // Map only
                openEmpty();
                mapCityFile(config.MAPPER_DATASET_PATHS.get(0), 0, false); // TODO Currently no support for importing entire folder
                summarize();
            }
            case 1 -> {
                // Export only
                openExisting();
                exportCityFile(0);
            }
            case 2 -> {
                // Map and export
                openEmpty();
                mapCityFile(config.MAPPER_DATASET_PATHS.get(0), 0, false); // TODO Currently no support for importing entire folder
                summarize();
                exportCityFile(0);
            }
            case 3 -> {
                // Open existing database for querying
                openExisting();
            }
            default -> throw new RuntimeException("Invalid use case");
        }
        close();
    }

    // TODO Currently no multithreaded read for CityJSON
    protected Neo4jRef mapCityFile(String filePath, int partitionIndex, boolean connectToRoot) {
        final Neo4jRef[] cityModelRef = {null};
        try {
            CityKGDBConfig cityGMLConfig = (CityKGDBConfig) config;
            if (!cityGMLConfig.CITYGML_VERSION.equals("json")) {
                logger.warn("No CityJSON option found in {}", ((CityKGDBConfig) config).CITYJSON);
            }
            dbStats.startTimer();
            CityJSONContext context = CityJSONContext.newInstance();
            CityJSONInputFactory in = context.createCityJSONInputFactory();
            Path file = Path.of(filePath);
            logger.info("Reading CityJSON file {} entirely into main memory", filePath);

            CityModel cityModel;
            try (CityJSONReader reader = in.createCityJSONReader(file)) {
                cityModel = (CityModel) reader.next();
            } catch (CityJSONReadException e) {
                throw new RuntimeException(e);
            }

            Neo4jRef graphRef = (Neo4jRef) this.map(cityModel,
                    AuxNodeLabels.__PARTITION_INDEX__.name() + partitionIndex);

            logger.info("Mapped all {} top-level features", cityModel.getCityObjectMembers().size());
            logger.info("Finished mapping file {}", filePath);
            dbStats.stopTimer("Map input file [" + partitionIndex + "]");

            mappedClassesTmp.clear(); // TODO Clear this so that summarize() can be called, since setIndexesIfNew() is not used
        } catch (CityJSONContextException e) {
            throw new RuntimeException(e);
        }
        return cityModelRef[0];
    }

    // TODO Currently no bbox support for exporting CityJSON
    public void exportCityFile(int partitionIndex) {
        try (Transaction tx = graphDb.beginTx()) {
            // Get the CityModel node
            Node cityModelNode = tx.findNodes(Label.label(CityModel.class.getName()))
                    .stream()
                    .filter(node -> node.hasLabel(
                            Label.label(AuxNodeLabels.__PARTITION_INDEX__.toString() + partitionIndex)))
                    .findFirst()
                    .get();

            CityJSONContext cityJSONContext = CityJSONContext.newInstance();
            CityJSONVersion version = CityJSONVersion.v2_0; // TODO Current version
            CityJSONOutputFactory out = cityJSONContext.createCityJSONOutputFactory(version)
                    .withVertexPrecision(3)
                    .withTextureVertexPrecision(5)
                    .applyTransformation(true);

            Metadata metadata = new Metadata();
            metadata.setTitle("CityJSON dataset exported with citygml4j");

            String exportFilePath = ((CityKGDBConfig) config).CITYGML_EXPORT_PATH;
            Path output = Path.of(exportFilePath);
            CityModel cityModel = (CityModel) toObject(cityModelNode, null);
            try (CityJSONWriter writer = out.createCityJSONWriter(output)) {
                writer.withIndent("  ")
                        .withMetadata(metadata)
                        .writeCityObject(cityModel);
            } catch (CityJSONWriteException e) {
                throw new RuntimeException(e);
            }

            logger.info("CityJSON file written to {}", exportFilePath);
            tx.commit();
        } catch (CityJSONContextException e) {
            throw new RuntimeException(e);
        }
    }
}

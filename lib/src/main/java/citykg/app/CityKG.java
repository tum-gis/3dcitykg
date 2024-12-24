package citykg.app;

import citykg.core.config.CityKGDBConfig;
import citykg.core.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class CityKG {
    private final static Logger logger = LoggerFactory.getLogger(CityKG.class);

    public static void main(String[] args) {
        String configFilePath = null;
        if (args.length < 1 || args[0] == null || args[0].trim().isEmpty()) {
            configFilePath = "config/run.conf";
        }
        File configFile = new File(configFilePath);

        if (!configFile.exists()) {
            logger.error("Config file not found: " + configFilePath);
            System.exit(1);
        }

        CityKGDBConfig config = new CityKGDBConfig(configFilePath);
        Neo4jDB neo4jDB;
        if (config.CITYJSON) {
            neo4jDB = new CityJSONDB(config);
        } else if (config.CITYGML_VERSION.equals("v1_0")) {
            neo4jDB = new CityGMLV2DB(config);
        } else if (config.CITYGML_VERSION.equals("v2_0")) {
            neo4jDB = new CityGMLV2DB(config);
        } else if (config.CITYGML_VERSION.equals("v3_0")) {
            neo4jDB = new CityGMLV3DB(config);
        } else {
            throw new RuntimeException("Dataset version given " + config.CITYGML_VERSION
                    + ", expected 'v1.0', 'v2.0', 'v3.0', or 'json'");
        }
        neo4jDB.go();
    }
}

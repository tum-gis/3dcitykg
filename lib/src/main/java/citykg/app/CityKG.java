package citykg.app;

import citykg.core.config.CityKGDBConfig;
import citykg.core.db.*;

public class CityKG {
    public static void main(String[] args) {
        CityKGDBConfig config = new CityKGDBConfig("config/run.conf");
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

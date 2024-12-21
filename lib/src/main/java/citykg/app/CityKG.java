package citykg.app;

import citykg.core.config.CityKGDBConfig;
import citykg.core.db.CityKGDB;
import citykg.core.db.citygml.CityV2;
import citykg.core.db.citygml.CityV3;
import org.citygml4j.core.model.CityGMLVersion;

public class CityKG {
    public static void main(String[] args) {
        CityKGDBConfig config = new CityKGDBConfig("config/run.conf");
        CityKGDB cityKGDB;
        if (config.CITYGML_VERSION == CityGMLVersion.v1_0) {
            cityKGDB = new CityV2(config);
        } else if (config.CITYGML_VERSION == CityGMLVersion.v2_0) {
            cityKGDB = new CityV2(config);
        } else if (config.CITYGML_VERSION == CityGMLVersion.v3_0) {
            cityKGDB = new CityV3(config);
        } else {
            throw new RuntimeException("CityGML version given " + config.CITYGML_VERSION
                    + ", expected " + CityGMLVersion.v1_0
                    + ", " + CityGMLVersion.v2_0
                    + " or " + CityGMLVersion.v3_0);
        }
        cityKGDB.go();
    }
}

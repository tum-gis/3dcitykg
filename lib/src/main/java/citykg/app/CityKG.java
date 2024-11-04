package citykg.app;

import citykg.core.config.CityKGDBConfig;
import citykg.core.db.citygml.CityGMLDBV2;
import citykg.core.db.citygml.CityGMLDBV3;

public class CityKG {
    private static void mapCityGMLV2() throws InterruptedException {
        CityGMLDBV2 cityGMLNeo4jDB = new CityGMLDBV2(new CityKGDBConfig("config/citygmlv2.conf")); // testMapv2.conf
        cityGMLNeo4jDB.open();
        cityGMLNeo4jDB.mapFromConfig();
        cityGMLNeo4jDB.summarize();
        //cityGMLNeo4jDB.close();
        cityGMLNeo4jDB.remainOpen();
    }

    private static void mapCityGMLV3() throws InterruptedException {
        CityGMLDBV3 cityGMLNeo4jDB = new CityGMLDBV3(new CityKGDBConfig("config/citygmlv3.conf"));
        cityGMLNeo4jDB.open();
        cityGMLNeo4jDB.mapFromConfig();
        cityGMLNeo4jDB.summarize();
        cityGMLNeo4jDB.close();
        //cityGMLNeo4jDB.remainOpen();
    }

    public static void main(String[] args) {
        try {
            mapCityGMLV2();
            // mapCityGMLV3();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

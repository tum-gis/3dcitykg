package citykg.app;

import citykg.core.config.CityKGDBConfig;
import citykg.core.db.citygml.CityV2;
import citykg.core.db.citygml.CityV3;

public class CityKG {
    private static void testCityGMLV2() throws InterruptedException {
        CityV2 cityV2 = new CityV2("config/citygmlv2.conf");
        cityV2.go();
    }

    private static void testCityGMLV3() throws InterruptedException {
        CityV3 cityV3 = new CityV3("config/citygmlv3.conf");
        cityV3.go();
    }

    public static void main(String[] args) {
        try {
            testCityGMLV2();
            // testCityGMLV3();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

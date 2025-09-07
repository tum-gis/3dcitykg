package citykg.core.config;

// Map CityGML datasets onto graphs using neo4j database
public class CityKGDBConfig extends Neo4jDBConfig {
    public final String CITYGML_VERSION;
    public final boolean CITYJSON;
    public final Integer CITYGML_EXPORT_PARTITION;
    public final double[] CITYGML_EXPORT_BBOX;
    public final String CITYGML_EXPORT_PATH;

    public CityKGDBConfig(String configPath) {
        super(configPath);
        CITYGML_VERSION = config.getString("citygml.version");
        CITYJSON = config.getBoolean("cityjson");
        CITYGML_EXPORT_PARTITION = config.getInt("citygml.export.partition");
        String bboxString = config.getString("citygml.export.bbox");
        String[] bboxStringArray = bboxString.trim().split(",\\s*");
        if (bboxStringArray.length != 6) {
            CITYGML_EXPORT_BBOX = new double[]{-1e9, -1e9, -1e9, 1e9, 1e9, 1e9};
        } else {
            CITYGML_EXPORT_BBOX = new double[6];
            for (int i = 0; i < bboxStringArray.length; i++) {
                CITYGML_EXPORT_BBOX[i] = Double.parseDouble(bboxStringArray[i]);
            }
        }

        CITYGML_EXPORT_PATH = config.getString("citygml.export.path");
    }
}

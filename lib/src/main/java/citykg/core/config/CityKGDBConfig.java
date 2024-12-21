package citykg.core.config;

import org.citygml4j.core.model.CityGMLVersion;

import java.util.Arrays;

// Map CityGML datasets onto graphs using neo4j database
public class CityKGDBConfig extends Neo4jDBConfig {
    public final CityGMLVersion CITYGML_VERSION;
    public final Integer CITYGML_EXPORT_PARTITION;
    public final double[] CITYGML_EXPORT_BBOX;
    public final String CITYGML_EXPORT_PATH;

    public CityKGDBConfig(String configPath) {
        super(configPath);
        CITYGML_VERSION = config.getEnum(CityGMLVersion.class, "citygml.version");
        CITYGML_EXPORT_PARTITION = config.getInt("citygml.export.partition");
        String bboxString = config.getString("citygml.export.bbox");
        String[] bboxStringArray = bboxString.trim().split(",\\s*");
        CITYGML_EXPORT_BBOX = new double[bboxStringArray.length];
        for (int i = 0; i < bboxStringArray.length; i++) {
            CITYGML_EXPORT_BBOX[i] = Double.parseDouble(bboxStringArray[i]);
        }
        CITYGML_EXPORT_PATH = config.getString("citygml.export.path");
    }
}

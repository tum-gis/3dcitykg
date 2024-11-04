package citykg.core.config;

import org.citygml4j.core.model.CityGMLVersion;

// Map CityGML datasets onto graphs using neo4j database
public class CityKGDBConfig extends Neo4jDBConfig {
    public final CityGMLVersion CITYGML_VERSION;

    public CityKGDBConfig(String configPath) {
        super(configPath);
        CITYGML_VERSION = config.getEnum(CityGMLVersion.class, "citygml.version");
    }
}

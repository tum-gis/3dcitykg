package citykg.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class BaseDBConfig {
    protected Config config;
    public final String DB_PATH;
    public final String DB_NAME;
    public final int DB_BATCH_SIZE;
    public final List<String> MAPPER_DATASET_PATHS;
    public final Set<Class<?>> MAPPER_EXCLUDE_VERTEX_CLASSES;
    public final Set<String> MAPPER_EXCLUDE_EDGE_TYPES;
    public final int MAPPER_CONCURRENT_TIMEOUT;
    public final int MAPPER_TOPLEVEL_BATCH_SIZE;

    public BaseDBConfig(String configPath) {
        Config parsedConfig = ConfigFactory.parseFile(new File(configPath));
        config = ConfigFactory.load(parsedConfig);
        DB_PATH = config.getString("db.path");
        DB_NAME = config.getString("db.name");
        DB_BATCH_SIZE = config.getInt("db.batch.size");
        MAPPER_DATASET_PATHS = config.getStringList("mapper.dataset.paths");
        MAPPER_EXCLUDE_VERTEX_CLASSES = new HashSet<>();
        config.getStringList("mapper.exclude.vertex.classes").forEach(label -> {
            try {
                MAPPER_EXCLUDE_VERTEX_CLASSES.add(Class.forName(label));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
        MAPPER_EXCLUDE_EDGE_TYPES = new HashSet<>();
        MAPPER_EXCLUDE_EDGE_TYPES.addAll(config.getStringList("mapper.exclude.edge.types"));
        MAPPER_CONCURRENT_TIMEOUT = config.getInt("mapper.concurrent.timeout");
        MAPPER_TOPLEVEL_BATCH_SIZE = config.getInt("mapper.toplevel.batch.size");
    }
}

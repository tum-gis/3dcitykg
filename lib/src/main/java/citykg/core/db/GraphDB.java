package citykg.core.db;

public interface GraphDB {
    void open();

    void openExisting();

    void close();
}

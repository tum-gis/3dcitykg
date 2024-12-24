package citykg.core.db;

public interface GraphDB {
    void openEmpty();

    void openExisting();

    void close();
}

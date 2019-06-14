package nl.sidnlabs.entrada.engine;

public interface QueryEngine {

  boolean executeSql(String sql);

  boolean addPartition(String table, int year, int month, int day, String server, String location);

}

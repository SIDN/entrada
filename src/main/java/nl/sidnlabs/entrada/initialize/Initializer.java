package nl.sidnlabs.entrada.initialize;

public interface Initializer {

  /**
   * Create the directories required for ENTRADA to function.
   * 
   * @return true if directories could be created ok
   */
  boolean createStorage();

  /**
   * Create the database and tables required for ENTRADA to function.
   * 
   * @return true if the database and tables could be created ok
   */
  boolean createDatabase();
}

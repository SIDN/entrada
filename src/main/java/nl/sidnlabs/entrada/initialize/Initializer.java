package nl.sidnlabs.entrada.initialize;

public interface Initializer {

  /**
   * Create the directories required for ENTRADA to function.
   * 
   * @return true if storage initialization completed ok
   * 
   */
  boolean initializeStorage();

  /**
   * Create the database and tables required for ENTRADA to function.
   * 
   * @return true if database initialization completed ok
   * 
   */
  boolean initializeDatabase();
}

package nl.sidnlabs.entrada.initialize;

public interface Initializer {

  /**
   * Create the directories required for ENTRADA to function.
   * 
   */
  boolean initializeStorage();

  /**
   * Create the database and tables required for ENTRADA to function.
   * 
   */
  boolean initializeDatabase();
}

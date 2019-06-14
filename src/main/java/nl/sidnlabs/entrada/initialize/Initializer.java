package nl.sidnlabs.entrada.initialize;

public interface Initializer {

  /**
   * Create the directories required for ENTRADA to function.
   * 
   */
  void createStorage();

  /**
   * Create the database and tables required for ENTRADA to function.
   * 
   */
  void createDatabase();
}

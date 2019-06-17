package nl.sidnlabs.entrada.load;

import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.Row;

public interface RowWriter {

  /**
   * create a parquet record which combines values from the query and the response
   * 
   * @param packet
   */
  public abstract Partition write(Row row, String server);

  public void open(String outputDir, String server, String name);

  public void close();

}

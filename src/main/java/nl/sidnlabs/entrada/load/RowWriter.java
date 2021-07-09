package nl.sidnlabs.entrada.load;

import java.util.Set;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.ProtocolType;
import nl.sidnlabs.entrada.model.Row;

public interface RowWriter {

  public abstract void write(Row row, String server);

  public void open(String outputDir, String server, String name);

  public void close();

  public Set<Partition> getPartitions();

  public ProtocolType type();

  public void printStats();

}

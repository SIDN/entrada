package nl.sidnlabs.entrada.load;

import java.util.Set;
import org.apache.avro.generic.GenericRecord;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.ProtocolType;

public interface RowWriter {

  public abstract void write(GenericRecord record, String server);

  public void open();

  public void close();

  public Set<Partition> getPartitions();

  public ProtocolType type();

  public void printStats();

}

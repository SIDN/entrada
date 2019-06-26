package nl.sidnlabs.entrada.engine;

import java.util.Set;
import java.util.concurrent.Future;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;

public interface QueryEngine {

  boolean execute(String sql);

  Future<Boolean> addPartition(String table, Set<Partition> partitions);

  boolean compact(TablePartition p);

  String compactionLocation(TablePartition p);

  String tableLocation(TablePartition p);

  boolean postCompact(TablePartition p);
}

package nl.sidnlabs.entrada.engine;

import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;

public interface QueryEngine {

  boolean execute(String sql);

  boolean addPartition(String type, String table, Partition partition);

  boolean postAddPartition(String table, Partition p);

  boolean compact(TablePartition p);

  String compactionLocation(TablePartition p);

  String tableLocation(TablePartition p);

  boolean preCompact(TablePartition p);

  boolean postCompact(TablePartition p);

  boolean purge(TablePartition p);

  boolean postPurge(TablePartition p);
}

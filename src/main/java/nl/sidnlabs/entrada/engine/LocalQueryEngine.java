package nl.sidnlabs.entrada.engine;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;

@Component("localEngine")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "local")
public class LocalQueryEngine implements QueryEngine {

  @Override
  public boolean addPartition(String type, String table, Partition partition) {
    // do nothing
    return true;
  }

  @Override
  public String compactionLocation(TablePartition p) {
    // do nothing
    return "";
  }

  @Override
  public String tableLocation(TablePartition p) {
    // do nothing
    return "";
  }

  @Override
  public boolean execute(String sql) {
    // do nothing
    return true;
  }

  @Override
  public boolean compact(TablePartition p) {
    // do nothing
    return true;
  }

  @Override
  public boolean postCompact(TablePartition p) {
    // do nothing
    return true;
  }

  @Override
  public boolean preCompact(TablePartition p) {
    // do nothing
    return true;
  }

  @Override
  public boolean postAddPartition(String table, Partition p) {
    // do nothing
    return true;
  }

  @Override
  public boolean purge(TablePartition p) {
    // do nothing
    return true;
  }

  @Override
  public boolean postPurge(TablePartition p) {
    // do nothing
    return true;
  }

}

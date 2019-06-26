package nl.sidnlabs.entrada.engine;

import java.util.Set;
import java.util.concurrent.Future;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;

@Component("localEngine")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "local")
public class LocalQueryEngine implements QueryEngine {

  @Override
  public Future<Boolean> addPartition(String table, Set<Partition> partitions) {
    // do nothing
    return new AsyncResult<>(Boolean.TRUE);
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

}

package nl.sidnlabs.entrada;

import org.springframework.stereotype.Component;
import lombok.Data;
import nl.sidnlabs.entrada.api.ExecutionStatus;

@Component
@Data
public class SharedContext {

  private boolean enabled = true;

  private boolean executionStatus;
  private boolean compactionStatus;
  private boolean maintenanceStatus;

  public ExecutionStatus getStatus() {
    return ExecutionStatus
        .builder()
        .enabled(enabled)
        .execution(executionStatus)
        .compaction(compactionStatus)
        .maintenance(maintenanceStatus)
        .build();
  }

}

package nl.sidnlabs.entrada;

import org.springframework.stereotype.Component;
import lombok.Data;
import nl.sidnlabs.entrada.api.StatusController.StatusResult;

@Component
@Data
public class SharedContext {

  private boolean enabled = true;

  private boolean executionStatus;
  private boolean compactionStatus;
  private boolean maintenanceStatus;

  public StatusResult getStatus() {
    return StatusResult
        .builder()
        .enabled(enabled)
        .execution(executionStatus)
        .compaction(compactionStatus)
        .maintenance(maintenanceStatus)
        .build();
  }

}

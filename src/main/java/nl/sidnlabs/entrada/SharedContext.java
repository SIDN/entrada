package nl.sidnlabs.entrada;

import java.util.concurrent.Semaphore;
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
  private boolean privacyPurgeStatus;
  private final Semaphore tableUpdater = new Semaphore(1, true);

  public StatusResult getStatus() {
    return StatusResult
        .builder()
        .enabled(enabled)
        .execution(executionStatus)
        .compaction(compactionStatus)
        .maintenance(maintenanceStatus)
        .privacypurge(privacyPurgeStatus)
        .build();
  }

}

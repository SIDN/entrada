package nl.sidnlabs.entrada;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.service.ArchiveService;

@Log4j2
@Component
public class ScheduledMaintenance {

  private ArchiveService archiveService;
  private SharedContext sharedContext;

  // use entrada.parquet.compaction.enabled as as semi master node config option
  // we need option so only 1 instance act as master instance to prevent
  @Value("${entrada.node.master:true}")
  private boolean enabled;

  public ScheduledMaintenance(ArchiveService archiveService, SharedContext sharedContext) {
    this.archiveService = archiveService;
    this.sharedContext = sharedContext;
  }

  /**
   * Execute maintenance every x minutes but wait 1 minute before enabling the schedule
   */
  @Scheduled(fixedDelayString = "#{${entrada.maintenance.interval:3600}*60*1000}",
      initialDelay = 60 * 1000)
  public void run() {
    log.info("Start maintenance");

    if (!enabled || !sharedContext.isEnabled()) {
      // processing not enabled
      log.info("Maintenance is currently not enabled");
      return;
    }

    sharedContext.setMaintenanceStatus(true);

    // delete processed files that have expired
    archiveService.clean();

    sharedContext.setMaintenanceStatus(false);

    log.info("Finished maintenance");
  }

}

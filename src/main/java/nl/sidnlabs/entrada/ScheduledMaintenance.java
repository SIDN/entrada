package nl.sidnlabs.entrada;

import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.service.ArchiveService;

@Log4j2
@Component
public class ScheduledMaintenance {

  private ArchiveService archiveService;
  private SharedContext sharedContext;
  private List<FileManager> fileManagers;

  // we need this option so only 1 instance act as master instance to prevent
  @Value("${entrada.node.master:true}")
  private boolean master;

  public ScheduledMaintenance(ArchiveService archiveService, SharedContext sharedContext,
      List<FileManager> fileManagers) {
    this.archiveService = archiveService;
    this.sharedContext = sharedContext;
    this.fileManagers = fileManagers;
  }

  /**
   * Execute maintenance every x minutes but wait 1 minute before enabling the schedule
   */
  @Scheduled(fixedDelayString = "#{${entrada.maintenance.interval:15}*60*1000}",
      initialDelay = 60 * 1000)
  public void run() {
    if (!master || !sharedContext.isEnabled()) {
      // processing not enabled
      log.debug("Maintenance is currently not enabled");
      return;
    }

    log.info("Start maintenance");

    sharedContext.setMaintenanceStatus(true);

    // delete processed files that have expired
    try {
      archiveService.clean();
    } finally {
      sharedContext.setMaintenanceStatus(false);

      // cleanup filesystems, make sure all cached data and locked files are cleanup up
      fileManagers.stream().forEach(FileManager::close);
    }

    log.info("Finished maintenance");
  }

}

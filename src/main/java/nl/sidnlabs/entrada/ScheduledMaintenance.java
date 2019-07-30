package nl.sidnlabs.entrada;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.service.ArchiveService;

@Log4j2
@Component
public class ScheduledMaintenance {

  private ArchiveService archiveService;

  public ScheduledMaintenance(ArchiveService archiveService) {
    this.archiveService = archiveService;
  }

  /**
   * Execute maintenance every x minutes but wait 1 minute before enabling the schedule
   */
  @Scheduled(fixedDelayString = "#{${entrada.maintenance.interval:3600}*60*1000}",
      initialDelay = 60 * 1000)
  public void run() {
    log.info("Start maintenance");

    // clean file table, prevent building up a huge history
    archiveService.clean();

    log.info("Finished maintenance");
  }

}

package nl.sidnlabs.entrada;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.service.PartitionService;

@Log4j2
@Component
@ConditionalOnExpression("{'aws', 'hadoop'}.contains('${entrada.engine}')")
public class ScheduledPrivacyPurge {

  @Value("${entrada.privacy.enabled:false}")
  private boolean privacy;

  @Value("${entrada.privacy.purge.age:100}")
  private int maxDays;

  private PartitionService partitionService;
  private QueryEngine queryEngine;
  private SharedContext sharedContext;


  public ScheduledPrivacyPurge(PartitionService partitionService, QueryEngine queryEngine,
      SharedContext sharedContext) {
    this.partitionService = partitionService;
    this.queryEngine = queryEngine;
    this.sharedContext = sharedContext;
  }


  /**
   * Execute purge every x minutes but wait 60 secs before enabling the schedule, give the
   * application time to full initialize
   */
  @Scheduled(fixedDelayString = "#{${entrada.privacy.purge.interval:60}*60*1000}",
      initialDelay = 60 * 1000)
  public void run() {

    if (privacy || maxDays == 0 || !sharedContext.isEnabled()) {
      // not purging PII because, privacy is already enabled and then no PII is present in the data
      // OR
      // the purge is disable by setting maxDays to 0
      // OR
      // entrada is currently disabled
      log.debug("Do nothing: privacy purge not enabled or running with privacy enabled ");
      return;
    }

    int purged = 0;

    try {
      // make sure not running at the same time as the compaction task
      sharedContext.getTableUpdater().acquire();
      sharedContext.setPrivacyPurgeStatus(true);
      log.info("Start partition privacy purge");

      LocalDate dateToPurge = LocalDate.now().minusDays(maxDays);
      List<TablePartition> partitions = partitionService.unPurgedPartitions(dateToPurge);

      log.info("Found {} partition(s) that may need to be purged", partitions.size());
      for (TablePartition p : partitions) {
        if (shouldPurge(p) && !purge(p)) {
          // stop purging until the error cause is fixed
          return;
        }
        purged++;
      }
    } catch (Exception e) {
      log.error("Problem while trying to purge partitions", e);
    } finally {
      sharedContext.setPrivacyPurgeStatus(false);
      sharedContext.getTableUpdater().release();
    }

    log.info("Finished partition privacy purge, processed {} partitions", purged);
  }

  private boolean purge(TablePartition p) {
    log.info("Purge table: {} partition: {}", p.getTable(), p.getPath());

    Date start = new Date();
    try {
      if (queryEngine.purge(p)) {
        partitionService.purgedPartition(p, start, new Date(), true);
        return true;

      }
    } catch (Exception e) {
      log.error("Purging failed for table: {} and partition: {}", p.getTable(), p.getPath());
    }

    // mark the partition to indicate the purge process failed.
    partitionService.purgedPartition(p, start, new Date(), false);
    return false;
  }


  /**
   * Never purge current day and partitions that are actively written to. e.g. last update is less
   * then 1 day
   * 
   * @return true if safe to compact
   */
  private boolean shouldPurge(TablePartition p) {
    return LocalDate.of(p.getYear(), p.getMonth(), p.getDay()).isBefore(LocalDate.now())
        && DateUtils.addDays(p.getUpdated(), 1).before(new Date());
  }

}

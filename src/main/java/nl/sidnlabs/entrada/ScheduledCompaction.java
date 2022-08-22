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
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.service.PartitionService;

@Log4j2
@Component
@ConditionalOnExpression("{'aws', 'hadoop'}.contains('${entrada.engine}')")
public class ScheduledCompaction {

  @Value("${entrada.parquet.compaction.age}")
  private int age;

  @Value("${entrada.parquet.compaction.enabled}")
  private boolean enabled;

  @Value("${entrada.location.output}")
  private String outputLocation;

  // we need this option so only 1 instance act as master instance to prevent
  @Value("${entrada.node.master:true}")
  private boolean master;

  private PartitionService partitionService;
  private QueryEngine queryEngine;
  private SharedContext sharedContext;
  private List<FileManager> fileManagers;

  public ScheduledCompaction(PartitionService partitionService, QueryEngine queryEngine,
      SharedContext sharedContext, List<FileManager> fileManagers) {
    this.partitionService = partitionService;
    this.queryEngine = queryEngine;
    this.sharedContext = sharedContext;
    this.fileManagers = fileManagers;
  }


  /**
   * Execute compaction every x minutes but wait 1 minute before enabling the schedule, give the
   * application time to full initialize
   */
  @Scheduled(fixedDelayString = "#{${entrada.parquet.compaction.interval:1}*60*1000}",
      initialDelay = 60 * 1000)
  public void run() {

    if (!master || !enabled || !sharedContext.isEnabled()) {
      // this node is not the master node
      // OR
      // compaction not enabled
      // OR
      // entrada is not enabled
      log
          .debug("Do not start, state ivalid: master:{}, compaction-enabled:{}, age:{}, enabled:{}",
              master, enabled, age, sharedContext.isEnabled());
      return;
    }

    log.info("Start table compaction");
    int compacted = 0;

    try {

      // make sure not running at the same time as the purge task
      sharedContext.getTableUpdater().acquire();
      sharedContext.setCompactionStatus(true);

      List<TablePartition> partitions = partitionService.uncompactedPartitions();

      log.info("Found {} partition(s) that may need to be compacted", partitions.size());

      for (TablePartition p : partitions) {
        if (shouldCompact(p) && !compact(p)) {
          // stop compacting until the error cause is fixed
          return;
        }

        compacted++;
      }
    } catch (Exception e) {
      log.error("Problem while trying to compact partitions", e);
    } finally {
      sharedContext.setCompactionStatus(false);
      sharedContext.getTableUpdater().release();

      // cleanup filesystems, make sure all cached data and locked files are cleanup up
      fileManagers.stream().forEach(FileManager::close);
    }


    log.info("Finished partition compaction, processed {} partitions", compacted);
  }

  private boolean compact(TablePartition p) {
    log.info("Compact table: {} partition: {}", p.getTable(), p.getPath());

    Date start = new Date();
    try {
      if (queryEngine.compact(p)) {
        partitionService.closePartition(p, start, new Date(), true);
        return true;

      }
    } catch (Exception e) {
      log.error("Compacting failed for: {}", p, e);
    }

    // mark the partition tgo indicate the compaction process failed.
    partitionService.closePartition(p, start, new Date(), false);
    return false;
  }


  /**
   * Never compact current day and paritions that are actively written to. e.g. last update is less
   * then age minutes ago
   * 
   * @return true if safe to compact
   */
  private boolean shouldCompact(TablePartition p) {
    return LocalDate.of(p.getYear(), p.getMonth(), p.getDay()).isBefore(LocalDate.now())
        && DateUtils.addMinutes(p.getUpdated(), age).before(new Date());
  }

}

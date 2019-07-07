package nl.sidnlabs.entrada;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
public class ScheduledCompaction {

  @Value("${entrada.parquet.compaction.age}")
  private int age;

  @Value("${entrada.parquet.compaction.enabled}")
  private boolean enabled;

  private PartitionService partitionService;
  private QueryEngine queryEngine;


  public ScheduledCompaction(PartitionService partitionService, QueryEngine queryEngine) {
    this.partitionService = partitionService;
    this.queryEngine = queryEngine;
  }


  /**
   * Execute compaction every x minutes but wait 1 minute before enabling the schedule
   */
  @Scheduled(fixedDelayString = "#{${entrada.parquet.compaction.interval:1}*60*1000}",
      initialDelay = 60 * 1000)
  public void run() {

    if (!enabled) {
      // compaction not enabled
      log.info("Compaction not enabled, mark any uncompacted partition as compacted");
      partitionService.closeUncompactedPartitions();
      return;
    }

    log.info("Start partition compaction");

    List<TablePartition> partitions = partitionService.uncompactedPartitions();

    log.info("Found {} partition(s) that may need to be compacted", partitions.size());

    for (TablePartition p : partitions) {
      if (shouldCompact(p) && !compact(p)) {
        // stop compacting until the error is fixed
        return;
      }
    }

    log.info("Finished partition compaction");
  }

  private boolean compact(TablePartition p) {
    log.info("Compact table: {} partition: {}", p.getTable(), p.toPath());

    Date start = new Date();
    try {
      if (queryEngine.compact(p)) {
        Date end = new Date();
        // mark partition as compacted in db
        p.setCompaction(end);
        long diffInMillies = Math.abs(end.getTime() - start.getTime());
        int seconds = (int) TimeUnit.SECONDS.convert(diffInMillies, TimeUnit.MILLISECONDS);
        p.setCompactionTime(seconds);
        partitionService.save(p);
        return true;

      }
    } catch (Exception e) {
      log.error("Compacting failed for: {}", p);
    }
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

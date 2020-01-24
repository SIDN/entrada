package nl.sidnlabs.entrada.service;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.model.jpa.TablePartitionRepository;

@Log4j2
@Component
public class PartitionService {

  @Value("${entrada.engine}")
  private String engine;

  private TablePartitionRepository tablePartitionRepository;

  public PartitionService(TablePartitionRepository tablePartitionRepository) {
    this.tablePartitionRepository = tablePartitionRepository;
  }

  @Transactional
  public void create(String table, Set<Partition> partitions) {
    log.info("Save {} partition(s)", partitions.size());

    partitions.stream().forEach(p -> upsertPartion(table, p));
  }

  /**
   * Create new partition when it does not exist yet or update and existing partition with the last
   * access date.
   * 
   * @param table the table the partition is linked to
   * @param p partition to check
   */
  private void upsertPartion(String table, Partition p) {
    // only create partition if it is now yet in the db
    TablePartition tp = tablePartitionRepository.findByTableAndPath(table, p.toPath());
    if (tp == null) {
      log.info("Update table {} add {}", table, p);

      tp = TablePartition
          .builder()
          .engine(engine)
          .table(table)
          .year(p.getYear())
          .month(p.getMonth())
          .day(p.getDay())
          .server(p.getServer())
          .created(new Date())
          .path(p.toPath())
          .updated(new Date())
          .build();
    } else {
      // update existing partition
      tp.setUpdated(new Date());
    }

    save(tp);
  }

  @Transactional
  public void ping(String table, Set<Partition> partitions) {
    partitions.stream().forEach(p -> pingPartition(table, p));
  }

  /**
   * Update partition update data to indicate that the partition is still being actively written to.
   * 
   * @param table the table the partition belongs to
   * @param p the partition that is still being used
   */
  private void pingPartition(String table, Partition p) {
    TablePartition tp = tablePartitionRepository.findByTableAndPath(table, p.toPath());
    if (tp != null) {
      log.info("Ping partition: {}", p);
      // update existing partition only
      tp.setUpdated(new Date());
      save(tp);
    }
  }

  public List<TablePartition> uncompactedPartitions() {
    return tablePartitionRepository.findUnCompactedForEngine(engine);
  }

  public List<TablePartition> unPurgedPartitions(LocalDate date) {
    return tablePartitionRepository
        .findUnUnpurgedForEngine(engine, date.getYear(), date.getMonthValue(),
            date.getDayOfMonth());
  }

  @Transactional
  public void closePartition(TablePartition p, Date start, Date end, boolean ok) {
    // mark partition as compacted and save duration of partition process in db
    p.setOk(Boolean.valueOf(ok));
    p.setCompaction(end);
    long diffInMillies = Math.abs(end.getTime() - start.getTime());
    p.setCompactionTime((int) diffInMillies / 1000);
    save(p);
  }

  @Transactional
  public void purgedPartition(TablePartition p, Date start, Date end, boolean ok) {
    // mark partition as purged of PII attributes
    p.setPrivacyPurgeOk(Boolean.valueOf(ok));
    p.setPrivacyPurgeTs(end);
    long diffInMillies = Math.abs(end.getTime() - start.getTime());
    p.setPrivacyPurgeTime((int) diffInMillies / 1000);
    save(p);
  }

  @Transactional
  public void save(TablePartition p) {
    tablePartitionRepository.save(p);
  }

}

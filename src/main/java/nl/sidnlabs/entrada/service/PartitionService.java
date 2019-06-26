package nl.sidnlabs.entrada.service;

import java.util.Date;
import java.util.List;
import java.util.Set;
import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
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
    log.info("Save {} partitions", partitions.size());

    partitions.stream().forEach(p -> createPartion(table, p));

  }

  private void createPartion(String table, Partition p) {
    // only create partion if it is now yet in the db
    TablePartition tp = tablePartitionRepository.findByTableAndPath(table, p.toPath());
    if (tp == null) {

      TablePartition newPartition = TablePartition
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

      tablePartitionRepository.save(newPartition);
    } else {
      // update existing partition
      tp.setUpdated(new Date());
      tablePartitionRepository.save(tp);
    }

  }

  public List<TablePartition> uncompactedPartitions() {
    return tablePartitionRepository.findUnCompactedForEngine(engine);
  }

  @Transactional
  public void save(TablePartition p) {
    tablePartitionRepository.save(p);
  }

}

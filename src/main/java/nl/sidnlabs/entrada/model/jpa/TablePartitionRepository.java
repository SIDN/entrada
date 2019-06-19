package nl.sidnlabs.entrada.model.jpa;

import java.util.List;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TablePartitionRepository extends CrudRepository<TablePartition, Integer> {

  TablePartition findByTableAndPath(String table, String path);

  @Query(value = "SELECT * FROM entrada_partition WHERE compaction_ts IS NULL ORDER BY id ASC",
      nativeQuery = true)
  List<TablePartition> findUnCompacted();

}

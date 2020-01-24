package nl.sidnlabs.entrada.model.jpa;

import java.util.List;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface TablePartitionRepository extends CrudRepository<TablePartition, Integer> {

  TablePartition findByTableAndPath(String table, String path);

  @Query(
      value = "SELECT * FROM entrada_partition WHERE engine = :engine AND compaction_ts IS NULL AND compaction_ok IS NULL ORDER BY id ASC",
      nativeQuery = true)
  List<TablePartition> findUnCompactedForEngine(@Param("engine") String engine);

  @Query(
      value = "SELECT * FROM entrada_partition WHERE engine = :engine AND privacy_purge_ts IS NULL AND year = :year AND month = :month AND day = :day ORDER BY id ASC",
      nativeQuery = true)
  List<TablePartition> findUnUnpurgedForEngine(@Param("engine") String engine,
      @Param("year") int year, @Param("month") int month, @Param("day") int day);


}

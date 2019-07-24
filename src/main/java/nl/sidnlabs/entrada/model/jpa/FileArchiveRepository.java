package nl.sidnlabs.entrada.model.jpa;

import java.time.LocalDate;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface FileArchiveRepository extends CrudRepository<FileArchive, Integer> {

  FileArchive findByFileAndServer(String file, String server);

  @Modifying
  @Query(value = "DELETE FROM entrada_file_archive WHERE date_start < :max_date",
      nativeQuery = true)
  int deleteOlderThan(@Param("max_date") LocalDate max);

}

package nl.sidnlabs.entrada.model.jpa;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FileArchiveRepository extends CrudRepository<FileArchive, Integer> {

  FileArchive findByFileAndServer(String file, String server);

}

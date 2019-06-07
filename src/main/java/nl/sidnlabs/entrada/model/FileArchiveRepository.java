package nl.sidnlabs.entrada.model;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FileArchiveRepository extends CrudRepository<FileArchive, Integer> {

  FileArchive findByFile(String file);

}

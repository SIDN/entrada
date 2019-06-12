package nl.sidnlabs.entrada.service;

import java.io.File;
import javax.transaction.Transactional;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.model.jpa.FileArchive;
import nl.sidnlabs.entrada.model.jpa.FileArchiveRepository;

@Component
public class FileArchiveService {

  private FileArchiveRepository fileArchiveRepository;

  public FileArchiveService(FileArchiveRepository fileArchiveRepository) {
    this.fileArchiveRepository = fileArchiveRepository;
  }

  public boolean exists(String file) {
    File f = new File(file);
    return fileArchiveRepository.findByFile(f.getName()) != null;
  }

  @Transactional
  public void save(FileArchive fa) {
    fileArchiveRepository.save(fa);
  }

}

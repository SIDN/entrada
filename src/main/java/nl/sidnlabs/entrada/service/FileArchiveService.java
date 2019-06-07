package nl.sidnlabs.entrada.service;

import java.io.File;
import java.util.Date;
import javax.transaction.Transactional;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.model.FileArchive;
import nl.sidnlabs.entrada.model.FileArchiveRepository;

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
  public void save(String file) {
    File f = new File(file);
    fileArchiveRepository.save(FileArchive.builder().date(new Date()).file(f.getName()).build());
  }

}

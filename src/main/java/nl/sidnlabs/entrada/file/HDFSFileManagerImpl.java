package nl.sidnlabs.entrada.file;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class HDFSFileManagerImpl implements FileManager {

  @Override
  public String schema() {
    return "hdfs://";
  }

  @Override
  public boolean exists(String file) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> files(String dir, String... filter) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Optional<InputStream> open(String filename) {
    // TODO Auto-generated method stub
    return null;
  }

}

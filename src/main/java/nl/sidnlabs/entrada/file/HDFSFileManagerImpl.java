package nl.sidnlabs.entrada.file;

import java.io.File;
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
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> files(String dir, String... filter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<InputStream> open(String filename) {
    throw new UnsupportedOperationException();
  }

  // @Override
  // public boolean write(String location) {
  // throw new UnsupportedOperationException();
  // }

  @Override
  public boolean move(File location, String outputLocation, boolean directory) {
    throw new UnsupportedOperationException();
  }


}

package nl.sidnlabs.entrada.file;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component("hdfs")
public class HDFSFileManagerImpl implements FileManager {

  private static final String HDFS_SCHEME = "hdfs://";

  @Override
  public String schema() {
    return HDFS_SCHEME;
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
  public Optional<InputStream> open(String location) {
    log.info("Open HDFS file: " + location);

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean upload(String location, String outputLocation, boolean archive) {
    log.info("Upload work location: {} to target location: {}", location, outputLocation);

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supported(String location) {
    try {
      URI uri = new URI(location);
      return StringUtils.equalsIgnoreCase(uri.getScheme(), "hdfs");
    } catch (URISyntaxException e) {
      log.error("Invalid location URI: " + location);
    }
    return false;
  }

  @Override
  public boolean delete(String location) {
    log.info("Delete HDFS file: " + location);

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean move(String src, String dest) {
    log.info("Move HDFS file: {} to: {} " + src, dest);

    throw new UnsupportedOperationException();
  }


  @Override
  public boolean isLocal() {
    return false;
  }


}

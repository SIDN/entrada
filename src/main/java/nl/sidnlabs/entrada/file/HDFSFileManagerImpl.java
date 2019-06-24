package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
@Component("hdfs")
public class HDFSFileManagerImpl implements FileManager {

  private static final String HDFS_SCHEME = "hdfs://";

  @Value("${hdfs.nameservice}")
  private String hdfsNameservice;

  @Value("${kerberos.username}")
  private String krbUsername;

  @Value("${kerberos.keytab}")
  private String krbKeyTab;

  @Override
  public String schema() {
    return HDFS_SCHEME;
  }

  @Override
  public boolean exists(String path) {
    FileSystem fs = createFS();
    try {
      return fs.exists(new Path(path));
    } catch (Exception e) {
      log.error("Error while checking existence of path: {}", path, e);
    }

    return false;
  }

  @Override
  public List<String> files(String dir, String... filter) {

    if (!exists(dir)) {
      log.error("Location {} does not exist, cannot continue");
      return Collections.emptyList();
    }

    FileSystem fs = createFS();

    try {
      return Arrays
          .stream(fs.listStatus(new Path(dir)))
          .map(s -> s.getPath().toString())
          .filter(p -> checkFilter(p, Arrays.asList(filter)))
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Error while checking for files in dir: {}", dir, e);
    }

    return Collections.emptyList();
  }

  private boolean checkFilter(String file, List<String> filters) {
    if (filters.isEmpty()) {
      return true;
    }
    return filters.stream().anyMatch(f -> StringUtils.endsWith(file, f));
  }

  @Override
  public Optional<InputStream> open(String location) {
    log.info("Open HDFS file: " + location);

    if (!exists(location)) {
      log.error("Location {} does not exist, cannot continue");
      return Optional.empty();
    }

    FileSystem fs = createFS();
    try {
      return Optional.of(fs.open(new Path(location)));
    } catch (Exception e) {
      log.error("Cannot open {}", location, e);
    }

    return Optional.empty();
  }

  @Override
  public boolean upload(String src, String dst, boolean archive) {
    log.info("Upload src location: {} to dst location: {}", src, dst);

    File f = new File(src);

    if (!f.exists()) {
      log.error("Location {} does not exist, cannot continue with upload");
      return false;
    }

    FileSystem fs = createFS();
    try {
      Path pathDst = new Path(dst);
      if (!fs.exists(pathDst)) {
        fs.mkdirs(pathDst);
      }
      fs.copyFromLocalFile(false, true, new Path(src), pathDst);
      return true;
    } catch (Exception e) {
      log.error("Error while uploading {} to {}", src, dst);
    }
    return false;
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

    FileSystem fs = createFS();
    try {
      Path path = new Path(location);
      // do not try to delete non-existing path, just return true
      if (fs.exists(path)) {
        return fs.delete(path, false);
      }

      return true;
    } catch (IllegalArgumentException | IOException e) {
      log.error("Cannot delete {} ", location, e);
    }

    return false;
  }

  @Override
  public boolean rmdir(String location) {
    log.info("Delete HDFS directory: " + location);

    FileSystem fs = createFS();
    try {
      Path path = new Path(location);
      // do not try to delete non-existing path, just return true
      if (fs.exists(path)) {
        return fs.delete(path, true);
      }

      return true;
    } catch (IllegalArgumentException | IOException e) {
      log.error("Cannot delete {} ", location, e);
    }

    return false;
  }

  @Override
  public boolean move(String src, String dst, boolean archive) {
    log.info("Move HDFS file: {} to: {} " + src, dst);

    FileSystem fs = createFS();
    try {
      return fs.rename(new Path(src), new Path(dst));
    } catch (IllegalArgumentException | IOException e) {
      log.error("Cannot rename {} to {}", src, dst, e);
    }

    return false;
  }


  @Override
  public boolean isLocal() {
    return false;
  }

  private FileSystem createNonSecureFS() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsNameservice);
    System.setProperty("HADOOP_USER_NAME", hdfsUser());

    try {
      return FileSystem.get(conf);
    } catch (IOException e) {
      throw new ApplicationException("Cannot create non-secure HDFS filesystem", e);
    }
  }

  private String hdfsUser() {
    String[] parts = StringUtils.split(krbUsername, "@");
    if (parts.length == 2) {
      return parts[1];
    }
    throw new ApplicationException(
        "Invalid kerberos username format, must be: <use>r@<REALM> found: " + krbUsername);
  }


  private FileSystem createSecureFS() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsNameservice);
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);

    try {
      if (StringUtils.isNotBlank(krbKeyTab)) {
        UserGroupInformation.loginUserFromKeytab(krbUsername, krbKeyTab);
      }

      return FileSystem.get(conf);
    } catch (IOException e) {
      throw new ApplicationException("Cannot create secure HDFS filesystem", e);
    }
  }

  private FileSystem createFS() {
    if (StringUtils.isNotBlank(krbUsername)) {
      // user using krb user/pass
      return createSecureFS();
    } else {
      // use on-secure
      return createNonSecureFS();
    }
  }

  @Override
  public boolean mkdir(String path) {
    log.info("Create directory: {}", path);

    FileSystem fs = createFS();
    try {
      return fs.mkdirs(new Path(path));
    } catch (Exception e) {
      log.error("Cannot create directory: {}", path, e);
    }
    return false;
  }

  @Override
  public boolean chown(String path, String owner, String group) {
    log.info("Chown directory: {} owner: {} group: {}", path, owner, group);

    FileSystem fs = createFS();
    try {
      fs.setOwner(new Path(path), owner, group);
      return true;
    } catch (Exception e) {
      log.error("Cannot chown directory: {}", path, e);
    }

    return true;
  }


}

package nl.sidnlabs.entrada.file;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
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

  @Value("${hdfs.username}")
  private String username;

  @Value("${kerberos.username}")
  private String krbUsername;

  @Value("${kerberos.password}")
  private String krbPassword;

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
  public boolean delete(String location, boolean children) {
    log.info("Delete HDFS file: " + location);

    throw new UnsupportedOperationException();
  }

  @Override
  public boolean move(String src, String dest, boolean archive) {
    log.info("Move HDFS file: {} to: {} " + src, dest);

    throw new UnsupportedOperationException();
  }


  @Override
  public boolean isLocal() {
    return false;
  }

  private FileSystem createNonSecureFS() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsNameservice);
    System.setProperty("HADOOP_USER_NAME", username);

    try {
      return FileSystem.get(conf);
    } catch (IOException e) {
      throw new ApplicationException("Cannot create non-secure HDFS filesystem", e);
    }
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

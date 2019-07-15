package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@Component("hdfs")
public class HDFSFileManagerImpl implements FileManager {

  private static final String HDFS_SCHEME = "hdfs://";

  @Value("${entrada.location.conf}")
  private String confDir;

  @Value("${hdfs.nameservice}")
  private String hdfsNameservice;

  @Value("${hdfs.username}")
  private String hdfsUsername;

  @Value("${kerberos.keytab}")
  private String krbKeyTab;

  @Value("${hdfs.data.owner}")
  private String owner;

  @Value("${hdfs.data.group}")
  private String group;

  @Override
  public String schema() {
    return HDFS_SCHEME;
  }

  @Override
  public boolean exists(String path) {
    return exists(createFS(), new Path(path));
  }

  private boolean exists(FileSystem fs, Path path) {
    try {
      return fs.exists(path);
    } catch (Exception e) {
      log.error("Error while checking existence of path: {}", path, e);
    }

    return false;
  }

  @Override
  public List<String> files(String dir, String... filter) {
    if (!exists(dir)) {
      log.error("Location {} does not exist, cannot continue", dir);
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
    Path pathSrc = new Path(src);
    Path pathDst = new Path(dst);

    if (!exists(fs, pathDst)) {
      mkdir(fs, pathDst);
    }

    if (f.isDirectory()) {
      uploadDir(fs, src, pathDst);
    } else {
      upload(fs, pathSrc, pathDst);
    }

    if (!archive) {
      // when uploading non-pcap data files set the correct hdfs permissions
      return chown(fs, dst, owner, group);
    }
    return true;
  }

  private boolean uploadDir(FileSystem fs, String src, Path dst) {
    // uploading a (sub)directory will fail if the dir already exists
    // at the destination, therefore upload each file individual and make
    // sure the directories exist.
    Set<Path> dirs = new HashSet<>();
    try (Stream<java.nio.file.Path> walk =
        Files.walk(Paths.get(src)).filter(p -> p.toFile().isFile())) {

      walk.forEach(p -> {
        Path srcPath = new Path(p.toString());
        Path dir = new Path(FileUtil
            .appendPath(dst.toString(),
                StringUtils.substringAfter(srcPath.getParent().toString(), src)));
        if (!dirs.contains(dir)) {
          // new dir, try op create
          mkdir(fs, dir);
          dirs.add(dir);
        }
        upload(fs, srcPath, new Path(FileUtil.appendPath(dir.toString(), srcPath.getName())));
      });
      return true;
    } catch (Exception e) {
      log.error("Error while uploading {} to {}", src, dst, e);
    }

    return false;
  }

  private boolean upload(FileSystem fs, Path src, Path dst) {
    try {
      fs.copyFromLocalFile(false, true, src, dst);
      return true;
    } catch (IOException e) {
      log.error("Error while uploading {} to {}", src, dst, e);
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
    log.info("Move HDFS file: {} to: {} ", src, dst);

    FileSystem fs = createFS();
    try {
      Path dstPath = new Path(dst);
      if (!fs.exists(dstPath.getParent())) {
        fs.mkdirs(dstPath.getParent());
      }
      return fs.rename(new Path(src), dstPath);
    } catch (IllegalArgumentException | IOException e) {
      log.error("Cannot rename {} to {}", src, dst, e);
    }

    return false;
  }


  @Override
  public boolean isLocal() {
    return false;
  }

  private Configuration conf() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", hdfsNameservice);


    String coreSiteXml = confDir + "/core-site.xml";
    if (!new File(coreSiteXml).exists()) {
      throw new ApplicationException("Missing core-site.xml, add this to the conf directory");
    }

    String hdfsSiteXml = confDir + "/hdfs-site.xml";
    if (!new File(hdfsSiteXml).exists()) {
      throw new ApplicationException("Missing hdfs-site.xml, add this to the conf directory");
    }

    conf.addResource(new Path("file://" + hdfsSiteXml));
    conf.addResource(new Path("file://" + coreSiteXml));

    return conf;
  }

  private FileSystem createNonSecureFS() {
    Configuration conf = conf();
    System.setProperty("HADOOP_USER_NAME", hdfsUsername);

    try {
      return FileSystem.get(conf);
    } catch (IOException e) {
      throw new ApplicationException("Cannot create non-secure HDFS filesystem", e);
    }
  }

  private FileSystem createSecureFS() {
    Configuration conf = conf();
    conf.set("hadoop.security.authentication", "kerberos");

    UserGroupInformation.setConfiguration(conf);

    try {
      if (StringUtils.isNotBlank(krbKeyTab)) {
        UserGroupInformation.loginUserFromKeytab(hdfsUsername, krbKeyTab);
      }

      return FileSystem.get(new URI(hdfsNameservice), conf);
    } catch (Exception e) {
      throw new ApplicationException("Cannot create secure HDFS filesystem", e);
    }
  }

  private FileSystem createFS() {
    if (StringUtils.isNotBlank(krbKeyTab)) {
      // user using krb user/pass
      return createSecureFS();
    } else {
      // use on-secure
      return createNonSecureFS();
    }
  }

  @Override
  public boolean mkdir(String path) {
    return mkdir(createFS(), new Path(path));
  }

  private boolean mkdir(FileSystem fs, Path path) {
    log.info("Create directory: {}", path);

    try {
      return fs.mkdirs(path);
    } catch (Exception e) {
      log.error("Cannot create directory: {}", path, e);
    }
    return false;
  }

  public boolean chown(String path, String owner, String group) {
    return chown(createFS(), path, owner, group);
  }

  private boolean chown(FileSystem fs, String path, String owner, String group) {
    log.info("Chown directory: {} owner: {} group: {}", path, owner, group);

    Path p = new Path(path);
    try {
      if (owner != null || group != null) {
        if (fs.isDirectory(p)) {
          for (FileStatus child : fs.listStatus(p)) {
            chown(child.getPath().toString(), owner, group);
          }
        }
        fs.setOwner(p, owner, group);
      }
    } catch (Exception e) {
      //
      return false;
    }


    return true;
  }

}

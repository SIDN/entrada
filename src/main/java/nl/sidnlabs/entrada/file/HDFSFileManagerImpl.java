package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
    try (FileSystem fs = createFS()) {
      return exists(fs, new Path(path));
    } catch (Exception e) {
      throw new ApplicationException("Error checking if file exists", e);
    }
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

    try (FileSystem fs = createFS()) {
      return Arrays
          .stream(fs.listStatus(new Path(dir)))
          .map(s -> s.getPath().toString())
          .filter(p -> checkFilter(p, Arrays.asList(filter)))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new ApplicationException("Error while checking for files in dir: " + dir, e);
    }
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

    try (FileSystem fs = createFS()) {
      return Optional.of(fs.open(new Path(location)));
    } catch (Exception e) {
      throw new ApplicationException("Cannot open: " + location, e);
    }
  }

  @Override
  public boolean upload(String src, String dst, boolean archive) {
    log.info("Upload src location: {} to dst location: {}", src, dst);

    File f = new File(src);

    if (!f.exists()) {
      log.error("Location {} does not exist, cannot continue with upload");
      return false;
    }

    try (FileSystem fs = createFS()) {
      Path pathSrc = new Path(src);
      Path pathDst = new Path(dst);

      if (!exists(fs, pathDst)) {
        mkdir(fs, pathDst);
      }

      if (f.isDirectory()) {
        uploadDir(fs, src, pathDst, archive);
      } else {
        upload(fs, pathSrc, pathDst, archive);
      }
      return true;
    } catch (Exception e) {
      throw new ApplicationException("Cannot upload, src: " + src + " dst: " + dst, e);
    }
  }

  private boolean uploadDir(FileSystem fs, String src, Path dst, boolean archive) {
    // uploading a (sub)directory will fail if the dir already exists
    // at the destination, therefore upload each file individual and make
    // sure the directories exist.

    if (log.isDebugEnabled()) {
      log.debug("Upload dir {} to {}", src, dst);
    }

    Set<Path> dirs = new HashSet<>();
    try (Stream<java.nio.file.Path> walk =
        Files.walk(Paths.get(src)).filter(p -> p.toFile().isFile())) {

      walk.forEach(p -> {
        if (log.isDebugEnabled()) {
          log.debug("Check if {} needs to be uploaded {}", p);
        }

        Path srcPath = new Path(p.toString());
        Path dir = new Path(FileUtil
            .appendPath(dst.toString(),
                StringUtils.substringAfter(srcPath.getParent().toString(), src)));
        if (!dirs.contains(dir)) {
          // new dir, try op create
          if (log.isDebugEnabled()) {
            log.debug("Create HDFS directory {}", dir);
          }

          mkdir(fs, dir);
          dirs.add(dir);
        }

        if (log.isDebugEnabled()) {
          log.debug("Upload file {}", srcPath);
        }

        upload(fs, srcPath, new Path(FileUtil.appendPath(dir.toString(), srcPath.getName())),
            archive);

        if (log.isDebugEnabled()) {
          log.debug("Completed uploading file {}", srcPath);
        }
      });

      if (log.isDebugEnabled()) {
        log.debug("Completed upload");
      }

      return true;
    } catch (Exception e) {
      log.error("Error while uploading {} to {}", src, dst, e);
    }

    return false;
  }

  private boolean upload(FileSystem fs, Path src, Path dst, boolean archive) {
    if (log.isDebugEnabled()) {
      log.debug("Upload file {} to {}", src, dst);
    }

    try {
      fs.copyFromLocalFile(false, true, src, dst);

      if (!archive) {
        // when uploading non-pcap data files set the correct hdfs permissions
        if (log.isDebugEnabled()) {
          log.debug("Setting correct file permissions to uploaded files");
        }
        chown(fs, dst.toString(), owner, group);
      }

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


    try (FileSystem fs = createFS()) {

      Path path = new Path(location);
      // do not try to delete non-existing path, just return true
      if (fs.exists(path)) {
        return fs.delete(path, false);
      }

      return true;
    } catch (IllegalArgumentException | IOException e) {
      throw new ApplicationException("Cannot delete location: " + location, e);
    }
  }

  @Override
  public boolean rmdir(String location) {
    log.info("Delete HDFS directory: " + location);

    try (FileSystem fs = createFS()) {
      Path path = new Path(location);
      // do not try to delete non-existing path, just return true
      if (fs.exists(path)) {
        return fs.delete(path, true);
      }

      return true;
    } catch (IllegalArgumentException | IOException e) {
      throw new ApplicationException("Cannot delete location: " + location, e);
    }
  }

  @Override
  public boolean move(String src, String dst, boolean archive) {
    log.info("Move HDFS file: {} to: {} ", src, dst);

    try (FileSystem fs = createFS()) {
      Path dstPath = new Path(dst);
      if (!fs.exists(dstPath.getParent())) {
        fs.mkdirs(dstPath.getParent());
      }
      return fs.rename(new Path(src), dstPath);
    } catch (Exception e) {
      throw new ApplicationException("Cannot rename, src: " + src + " dst: " + dst, e);
    }
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
    try (FileSystem fs = createFS()) {
      return mkdir(fs, new Path(path));
    } catch (Exception e) {
      return false;
    }

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

    try (FileSystem fs = createFS()) {
      return chown(fs, path, owner, group);
    } catch (Exception e) {
      throw new ApplicationException("Cannot chown, path: " + path, e);
    }
  }

  private boolean chown(FileSystem fs, String path, String owner, String group) {

    if (log.isDebugEnabled()) {
      log.debug("Chown permissions for path: {}", path);
    }

    Path p = new Path(path);
    try {
      if (owner != null && group != null) {
        FileStatus fStatus = fs.getFileStatus(p);

        if (fStatus.isDirectory()) {
          for (FileStatus child : fs.listStatus(p)) {
            chown(fs, child.getPath().toString(), owner, group);
          }
        }

        // check to see if the owner/group needs to be corrected
        if (!StringUtils.equals(owner, fStatus.getOwner())
            || !StringUtils.equals(group, fStatus.getGroup())) {

          log.info("Chown: {} owner: {} group: {}", path, owner, group);
          fs.setOwner(p, owner, group);
        }
      }
    } catch (Exception e) {
      log.error("Error while doing chown for {}", path, e);
      return false;
    }
    return true;
  }


  @Override
  public List<String> expired(String location, int maxAge, String... filter) {
    if (!exists(location)) {
      log.error("Location {} does not exist, cannot continue", location);
      return Collections.emptyList();
    }
    List<String> files = new ArrayList<>();

    try (FileSystem fs = createFS()) {

      RemoteIterator<LocatedFileStatus> fileStatusListIterator =
          fs.listFiles(new Path(location), true);

      while (fileStatusListIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusListIterator.next();
        files.add(fileStatus.getPath().toString());
      }
    } catch (Exception e) {
      throw new ApplicationException("Error while getting files", e);
    }
    // retrun found files, can be partial list in case of an exception
    return files
        .stream()
        .filter(p -> checkFilter(p, Arrays.asList(filter)))
        .collect(Collectors.toList());
  }

}

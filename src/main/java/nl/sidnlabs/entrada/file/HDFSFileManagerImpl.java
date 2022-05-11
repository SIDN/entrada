package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;

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

  @Value("${hdfs.data.dir.permission}")
  private String dirPermission;

  @Value("${hdfs.data.file.permission}")
  private String filePermission;


  @Override
  public String schema() {
    return HDFS_SCHEME;
  }

  /*
   * keep 1 FileSystem per thread, a fs cannot be shared between threads when thrad is done the FS
   * needs to be closed, this is not possible when other threads are suing the same FS obj.
   */
  private ThreadLocal<FileSystem> threadLocalValue = new ThreadLocal<>();

  @PostConstruct
  public void postConstruct() {
    log.debug("Created HDFS FS manager, this instance: {}", this.hashCode());
  }

  /**
   * Close FS, Hadoop FileSystem class has internal cache for all connections used by this
   * application. make sure this method only closes the current connection used by this class
   * instance. this is why this class uses scope: SCOPE_PROTOTYPE
   */
  @Override
  public void close() {
    // close filesystem
    FileSystem fs = threadLocalValue.get();
    if (fs != null) {
      // make sure next code running on this thread cannot use this FS obj
      threadLocalValue.remove();
      try {
        fs.close();
        fs = null;
      } catch (Exception e) {
        log.error("Error while closing filesystem", e);
      }
    }

    // do not close Hadoop FileSystem class internal cached instances
    // other threads might still be using cached instances
    // and get "connection closed" IOException

  }

  @Override
  public boolean exists(String path) {
    FileSystem fs = null;
    try {
      fs = createFS();
      return exists(fs, new Path(path));
    } catch (Exception e) {
      log.error("Error checking if file exists", e);
      return false;
    }
  }

  private boolean exists(FileSystem fs, Path path) {
    try {
      return fs.exists(path);
    } catch (Exception e) {
      log.error("Error while checking existence of path: {}", path, e);
      return false;
    }
  }

  @Override
  public List<String> files(String dir, boolean recursive, String... filter) {
    if (!exists(dir)) {
      log.error("Location {} does not exist, cannot continue", dir);
      return Collections.emptyList();
    }
    FileSystem fs = null;
    try {
      fs = createFS();
      return Arrays
          .stream(fs.listStatus(new Path(dir)))
          .map(s -> s.getPath().toString())
          .filter(p -> checkFilter(p, Arrays.asList(filter)))
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Error while checking for files in dir: " + dir, e);
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
    FileSystem fs = null;
    try {
      fs = createFS();
      return Optional.of(fs.open(new Path(location)));
    } catch (Exception e) {
      log.error("Cannot open: " + location, e);
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
    FileSystem fs = null;
    try {
      fs = createFS();
      Path pathSrc = new Path(src);
      Path pathDst = new Path(dst);

      if (!exists(fs, pathDst)) {
        mkdir(fs, pathDst);
      }
      return upload(fs, pathSrc, pathDst, archive);
    } catch (Exception e) {
      log.error("Cannot upload, src: " + src + " dst: " + dst, e);
      return false;
    }
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
          log.debug("Setting correct file permissions to uploaded file");
        }
        return chown(fs, dst.toString(), owner, group)
            && chmod(fs, dst.toString(), dirPermission, filePermission);
      }

    } catch (IOException e) {
      log.error("Error while uploading {} to {}", src, dst, e);
      return false;
    }

    return true;
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

    FileSystem fs = null;
    try {
      fs = createFS();
      Path path = new Path(location);
      // do not try to delete non-existing path, just return true
      if (fs.exists(path)) {
        return fs.delete(path, false);
      }

      return true;
    } catch (IllegalArgumentException | IOException e) {
      log.error("Cannot delete location: " + location, e);
      return false;
    }
  }

  @Override
  public boolean rmdir(String location) {
    log.info("Delete HDFS directory: " + location);

    FileSystem fs = null;
    try {
      fs = createFS();
      Path path = new Path(location);
      // do not try to delete non-existing path, just return true
      if (fs.exists(path)) {
        return fs.delete(path, true);
      }

      return true;
    } catch (IllegalArgumentException | IOException e) {
      log.error("Cannot delete location: " + location, e);
      return false;
    }
  }

  @Override
  public boolean move(String src, String dst, boolean archive) {
    log.info("Move HDFS file: {} to: {} ", src, dst);

    FileSystem fs = null;
    try {
      fs = createFS();
      Path dstPath = new Path(dst);
      if (!fs.exists(dstPath.getParent())) {
        fs.mkdirs(dstPath.getParent());
      }
      return fs.rename(new Path(src), dstPath);
    } catch (Exception e) {
      log.error("Cannot rename, src: " + src + " dst: " + dst, e);
      return false;
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
      // always create new fs object to prevent closing shared object in another thread
      // see:
      // https://stackoverflow.com/questions/20057881/hadoop-filesystem-closed-exception-when-doing-bufferedreader-close/20061797#20061797
      return FileSystem.newInstance(conf);
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
      // always create new fs object to prevent closing shared object in another thread
      // see:
      // https://stackoverflow.com/questions/20057881/hadoop-filesystem-closed-exception-when-doing-bufferedreader-close/20061797#20061797
      return FileSystem.newInstance(new URI(hdfsNameservice), conf);
    } catch (Exception e) {
      throw new ApplicationException("Cannot create secure HDFS filesystem", e);
    }
  }

  private FileSystem createFS() {
    FileSystem fs = threadLocalValue.get();
    if (fs != null) {
      return fs;
    }

    if (StringUtils.isNotBlank(krbKeyTab)) {
      // user using krb user/pass
      fs = createSecureFS();
    } else {
      // use on-secure
      fs = createNonSecureFS();
    }

    // link FS to current thread
    threadLocalValue.set(fs);

    return fs;
  }

  @Override
  public boolean mkdir(String path) {
    FileSystem fs = null;
    try {
      fs = createFS();
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
    FileSystem fs = null;
    try {
      fs = createFS();
      return chown(fs, path, owner, group);
    } catch (Exception e) {
      log.error("Cannot chown, path: " + path, e);
      return false;
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

  public boolean chmod(String path, String permDir, String permFile) {
    FileSystem fs = null;
    try {
      fs = createFS();
      return chmod(fs, path, permDir, permFile);
    } catch (Exception e) {
      log.error("Cannot chmod, path: " + path, e);
      return false;
    }
  }

  private boolean chmod(FileSystem fs, String path, String permDir, String permFile) {

    if (log.isDebugEnabled()) {
      log.debug("Chmod permissions for path: {}", path);
    }

    Path p = new Path(path);
    try {
      if (owner != null && group != null) {
        FileStatus fStatus = fs.getFileStatus(p);

        if (fStatus.isDirectory()) {
          for (FileStatus child : fs.listStatus(p)) {
            chmod(fs, child.getPath().toString(), permDir, permFile);
          }
        }

        if (fStatus.isDirectory()) {
          fs.setPermission(p, new FsPermission(permDir));
        } else {
          fs.setPermission(p, new FsPermission(permFile));
        }
      }
    } catch (Exception e) {
      log.error("Error while doing chmod for {}", path, e);
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
    FileSystem fs = null;
    long max = System.currentTimeMillis() - (maxAge * 24 * 60 * 60 * 1000);

    try {
      fs = createFS();
      RemoteIterator<LocatedFileStatus> fileStatusListIterator =
          fs.listFiles(new Path(location), true);

      while (fileStatusListIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusListIterator.next();

        if (fileStatus.getModificationTime() < max) {
          files.add(fileStatus.getPath().toString());
        }
      }
    } catch (Exception e) {
      log.error("Error while getting files", e);
      return Collections.emptyList();
    }

    // retrun found files, can be partial list in case of an exception
    return files
        .stream()
        .filter(p -> checkFilter(p, Arrays.asList(filter)))
        .collect(Collectors.toList());
  }

}

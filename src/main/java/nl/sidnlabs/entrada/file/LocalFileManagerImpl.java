package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.FileSystemUtils;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class LocalFileManagerImpl implements FileManager {

  private static final String LOCAL_SCHEME = "file://";


  @Override
  public String schema() {
    return LOCAL_SCHEME;
  }

  @Override
  public boolean exists(String file) {
    File f = new File(file);
    return f.exists();
  }

  @Override
  public List<String> files(String dir, String... filter) {

    Spliterator<File> spliterator = Spliterators
        .spliteratorUnknownSize(FileUtils.iterateFiles(new File(dir), filter, false),
            Spliterator.NONNULL);

    return StreamSupport
        .stream(spliterator, false)
        .filter(File::isFile)
        .map(File::getAbsolutePath)
        .collect(Collectors.toList());

  }

  @Override
  public Optional<InputStream> open(String location) {
    log.info("Open local file: " + location);

    File f = FileUtils.getFile(location);
    try {
      return Optional.of(FileUtils.openInputStream(f));
    } catch (IOException e) {
      log.error("Cannot open file: {}", f, e);
    }

    return Optional.empty();
  }

  @Override
  public boolean upload(String location, String outputLocation, boolean archive) {
    log.info("Upload work location: {} to target location: {}", location, outputLocation);

    Path src = Paths.get(location);
    Path dest = Paths.get(outputLocation);

    try {
      Files.walk(src).forEach(s -> copy(s, src, dest));
      return true;
    } catch (Exception ex) {
      log.error("Cannot copy directory: {}", src, ex);
    }

    return false;
  }

  private void copy(Path s, Path src, Path dest) {
    try {
      Path d = dest.resolve(src.relativize(s));
      if (s.toFile().isDirectory()) {
        if (!d.toFile().exists())
          Files.createDirectory(d);
        return;
      }
      // copy when on same fs, move when src and dest are on different fs systems
      Files.move(s, d);
    } catch (Exception e) {
      log.error("Cannot copy file: {}", s, e);
    }
  }

  @Override
  public boolean delete(String location) {
    log.info("Delete local file: " + location);

    File f = new File(location);
    if (f.exists()) {
      return FileSystemUtils.deleteRecursively(f);
    }

    return false;
  }


  @Override
  public boolean move(String src, String dst) {
    log.info("Move local file: {} to: {} " + src, dst);

    try {
      Files.move(Paths.get(src), Paths.get(dst));
      return true;
    } catch (IOException e) {
      log.error("Error while moving file {} to {}", src, dst, e);
    }
    return false;
  }

  @Override
  public boolean supported(String location) {
    try {
      URI uri = new URI(location);
      return StringUtils.equalsIgnoreCase(uri.getScheme(), LOCAL_SCHEME);
    } catch (URISyntaxException e) {
      log.error("Invalid location URI: " + location);
    }
    return false;
  }

  @Override
  public boolean isLocal() {
    return true;
  }
}

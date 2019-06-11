package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class LocalFileManagerImpl implements FileManager {

  @Override
  public String schema() {
    return "file://";
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
  public Optional<InputStream> open(String filename) {
    File f = FileUtils.getFile(filename);
    try {
      return Optional.of(FileUtils.openInputStream(f));
    } catch (IOException e) {
      log.error("Cannot open file: {}", f, e);
    }

    return Optional.empty();
  }



}

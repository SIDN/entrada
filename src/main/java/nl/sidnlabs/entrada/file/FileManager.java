package nl.sidnlabs.entrada.file;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface FileManager {

  String schema();

  boolean exists(String file);

  List<String> files(String dir, String... filter);

  Optional<InputStream> open(String filename);

}

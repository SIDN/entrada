package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface FileManager {

  String schema();

  boolean exists(String location);

  List<String> files(String location, String... filter);

  Optional<InputStream> open(String location);

  boolean move(File location, String outputLocation, boolean directory);

}

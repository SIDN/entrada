package nl.sidnlabs.entrada.file;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class FileManagerFactoryImpl implements FileManagerFactory {

  private Map<String, FileManager> availableManagers = new HashMap<>();

  public FileManagerFactoryImpl(List<FileManager> managers) {
    managers.stream().forEach(m -> availableManagers.put(m.schema(), m));
  }

  @Override
  public FileManager getFor(String file) {

    return availableManagers
        .entrySet()
        .stream()
        .filter(e -> StringUtils.startsWith(file, e.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(availableManagers.get("file://"));
  }

}

package nl.sidnlabs.entrada.initialize;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.file.FileManagerFactory;

@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "local")
public class LocalInitializer extends AbstractInitializer {

  public LocalInitializer(FileManagerFactory fileManagerFactory) {
    super(null, null, fileManagerFactory);
  }

  @Override
  public boolean initializeDatabase() {
    // do not initialize a db in local mode
    return true;
  }

}

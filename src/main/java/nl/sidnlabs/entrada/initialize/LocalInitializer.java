package nl.sidnlabs.entrada.initialize;

import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;

@Log4j2
@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "local")
public class LocalInitializer implements Initializer {

  @Value("${entrada.location.work}")
  private String work;

  @Value("${entrada.location.input}")
  private String input;

  @Value("${entrada.location.output}")
  private String output;

  @Value("${entrada.location.archive}")
  private String archive;

  private FileManager fileManager;

  public LocalInitializer(@Qualifier("local") FileManager fileManager) {
    this.fileManager = fileManager;
  }

  @PostConstruct
  public void init() {
    log.info("Perform local provisioning");

    if (fileManager.supported(work) && !fileManager.mkdir(work)) {
      throw new ApplicationException("Cannot create work location: " + work);
    }

    if (fileManager.supported(input) && !fileManager.mkdir(input)) {
      throw new ApplicationException("Cannot create input location: " + input);
    }

    if (fileManager.supported(output) && !fileManager.mkdir(output)) {
      throw new ApplicationException("Cannot create output location: " + output);
    }

    if (fileManager.supported(archive) && !fileManager.mkdir(archive)) {
      throw new ApplicationException("Cannot create output location: " + archive);
    }
  }

  @Override
  public boolean initializeStorage() {
    return true;
  }

  @Override
  public boolean initializeDatabase() {
    // do not initialize a db in local mode
    return true;
  }

}

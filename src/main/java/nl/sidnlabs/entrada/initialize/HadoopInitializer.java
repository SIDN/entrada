package nl.sidnlabs.entrada.initialize;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;

@Log4j2
@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
public class HadoopInitializer extends AbstractInitializer {

  private FileManager fileManager;

  public HadoopInitializer(@Qualifier("hdfs") FileManager fileManager,
      @Qualifier("impala") QueryEngine queryEngine, FileManagerFactory fileManagerFactory) {

    super(queryEngine, "impala", fileManagerFactory);
    this.fileManager = fileManager;
  }

  @Override
  public boolean initializeStorage() {
    log.info("Provision Hadoop storage");

    // create local storage locations
    super.initializeStorage();

    if (!fileManager.supported(output)) {
      throw new ApplicationException(
          "Selected mode is Hadoop but the ENTRADA output location does not use HDFS, cannot continue");
    }

    if ((fileManager.supported(input) && !fileManager.exists(input)) && !fileManager.mkdir(input)) {
      return false;
    }

    if ((fileManager.supported(output) && !fileManager.exists(output))
        && (!fileManager.mkdir(output) || !fileManager.chown(output, false)
            || !fileManager.chmod(output, false))) {
      return false;
    }

    if ((fileManager.supported(archive) && !fileManager.exists(archive))
        && !fileManager.mkdir(archive)) {
      return false;
    }

    return true;
  }


}

package nl.sidnlabs.entrada.initialize;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;

@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
public class HadoopInitializer extends AbstractInitializer {

  @Value("${entrada.location.input}")
  private String inputLocation;

  @Value("${entrada.location.archive}")
  private String archiveLocation;

  private FileManager fileManager;

  public HadoopInitializer(@Qualifier("hdfs") FileManager fileManager,
      @Qualifier("impala") QueryEngine queryEngine,
      @Value("${entrada.icmp.enable}") boolean icmpEnabled) {

    super(queryEngine, "/sql/impala");
    this.fileManager = fileManager;
  }

  @Override
  public boolean initializeStorage() {
    if (!fileManager.supported(outputLocation)) {
      throw new ApplicationException(
          "Selected mode is Hadoop but the ENTRADA output location does not use HDFS, cannot continue");
    }

    if ((fileManager.supported(inputLocation) && !fileManager.exists(inputLocation))
        && !fileManager.mkdir(inputLocation)) {
      return false;
    }

    if ((fileManager.supported(outputLocation) && !fileManager.exists(outputLocation))
        && (!fileManager.mkdir(outputLocation)
            || !fileManager.chown(outputLocation, "impala", "impala"))) {
      return false;
    }

    if ((fileManager.supported(archiveLocation) && !fileManager.exists(archiveLocation))
        && !fileManager.mkdir(archiveLocation)) {
      return false;
    }

    return true;
  }


}

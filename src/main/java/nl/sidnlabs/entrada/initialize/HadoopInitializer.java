package nl.sidnlabs.entrada.initialize;

import javax.annotation.PostConstruct;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
public class HadoopInitializer implements Initializer {

  @PostConstruct
  public void init() {
    log.info("Perform Hadoop provisioning");
  }

  @Override
  public void createStorage() {
    // TODO Auto-generated method stub

  }

  @Override
  public void createDatabase() {
    // TODO Auto-generated method stub

  }

}

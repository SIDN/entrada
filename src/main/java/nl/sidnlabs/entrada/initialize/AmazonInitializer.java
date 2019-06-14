package nl.sidnlabs.entrada.initialize;

import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AmazonInitializer implements Initializer {

  private JdbcTemplate jdbcTemplate;

  public AmazonInitializer(@Qualifier("athenaJdbcTemplate") JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @PostConstruct
  public void init() {
    log.info("Perform AWS provisioning");
  }

  @Override
  public boolean createStorage() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean createDatabase() {
    // TODO Auto-generated method stub
    return false;
  }

}

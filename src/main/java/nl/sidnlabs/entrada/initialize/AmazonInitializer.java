package nl.sidnlabs.entrada.initialize;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class AmazonInitializer implements Initializer {

  private JdbcTemplate jdbcTemplate;

  public AmazonInitializer(
      @Qualifier("athenaJdbcTemplate") ObjectProvider<JdbcTemplate> jdbcProvider) {

    this.jdbcTemplate = jdbcProvider.getIfAvailable();
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

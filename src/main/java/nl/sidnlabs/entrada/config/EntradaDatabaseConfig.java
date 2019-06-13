package nl.sidnlabs.entrada.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class EntradaDatabaseConfig {

  @Autowired
  private Environment env;

  /**
   * Create primary database as bean so we set it as primary othwise flyway might connect to the
   * wrong data source.
   * 
   * @return
   */
  @Primary
  @Bean(name = "dataSource")
  @ConfigurationProperties(prefix = "spring.datasource")
  public DriverManagerDataSource dataSource() {
    log.info("Create ENTRADA datasource");

    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(env.getProperty("spring.datasource.driverClassName"));
    dataSource.setUrl(env.getProperty("spring.datasource.jdbcUrl"));
    dataSource.setUsername(env.getProperty("spring.datasource.username"));
    dataSource.setPassword(env.getProperty("spring.datasource.password"));

    return dataSource;

  }
}

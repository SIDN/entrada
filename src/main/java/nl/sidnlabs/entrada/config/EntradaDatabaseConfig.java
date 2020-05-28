package nl.sidnlabs.entrada.config;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.log4j.Log4j2;

/**
 * Create primary database as bean so we set it as primary otherwise flyway might connect to the
 * wrong data source. If we just use the config from application.properties and let Spring config
 * the datasource then the wrong db is setup as primary db.
 */
@Log4j2
@Configuration
public class EntradaDatabaseConfig {


  public EntradaDatabaseConfig() {}


  @Bean
  @ConfigurationProperties(prefix = "spring.datasource.hikari")
  public HikariConfig hikariConfig() {
    return new HikariConfig();
  }

  @Primary
  @Bean(name = "dataSource")
  public DataSource dataSource() throws SQLException {
    log.info("Create ENTRADA datasource");
    return new HikariDataSource(hikariConfig());
  }

}

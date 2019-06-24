package nl.sidnlabs.entrada.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class EntradaDatabaseConfig {

  /**
   * Create primary database as bean so we set it as primary otherwise flyway might connect to the
   * wrong data source. If we just use the config from application.properties and let Spring config
   * the datasource then the wrong db is setup as primary db.
   * 
   * @return datasource for entrada database.
   */
  @Primary
  @Bean(name = "dataSource")
  @ConfigurationProperties(prefix = "spring.datasource")
  public HikariDataSource dataSource(@Value("${spring.datasource.driver-class-name}") String driver,
      @Value("${spring.datasource.url}") String url,
      @Value("${spring.datasource.username}") String username,
      @Value("${spring.datasource.password}") String password) {
    log.info("Create ENTRADA datasource");

    return DataSourceBuilder
        .create()
        .type(HikariDataSource.class)
        .driverClassName(driver)
        .url(url)
        .username(username)
        .password(password)
        .build();
  }
}

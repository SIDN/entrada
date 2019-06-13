package nl.sidnlabs.entrada.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import com.simba.athena.jdbc.DataSource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
@Configuration
public class AthenaDatabaseConfig {

  private static final String AWS_CREDENTIALS_PROVIDER_CLASS =
      "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain";
  private static final String ATHENA_DRIVER_CLASS = "com.simba.athena.jdbc.Driver";

  @Value("${athena.workgroup}")
  private String workgroup;

  @Value("${athena.url}")
  private String url;

  @Value("${athena.log.path}")
  private String logPath;

  @Value("${athena.output.location}")
  private String outputLocation;

  @Value("${athena.active}")
  private boolean active;

  @Bean(name = "athenaDS")
  @ConditionalOnProperty(name = "athena.active")
  public DataSource athenaDataSource() {
    log.info("Create Athena datasource");

    try {
      Class.forName(ATHENA_DRIVER_CLASS);
    } catch (ClassNotFoundException e) {
      log.error("Failed to load Athena JDBC driver", e);
      throw new ApplicationException("Failed to load Athena JDBC driver", e);
    }
    DataSource ds = new com.simba.athena.jdbc.DataSource();
    ds.setCustomProperty("LogPath", logPath);
    ds.setCustomProperty("LogLevel", "4");
    ds.setCustomProperty("S3OutputLocation", outputLocation);
    ds.setCustomProperty("AwsCredentialsProviderClass", AWS_CREDENTIALS_PROVIDER_CLASS);
    ds.setCustomProperty("Workgroup", workgroup);
    ds.setURL(url);
    return ds;
  }


  @Autowired
  @Bean(name = "athenaJdbcTemplate")
  @ConditionalOnProperty(name = "athena.active")
  public JdbcTemplate getJdbcTemplate(@Qualifier("athenaDS") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }


}

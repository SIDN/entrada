package nl.sidnlabs.entrada.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import com.simba.athena.jdbc.DataSource;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class AthenaDatabaseConfig {

  private static final String AWS_CREDENTIALS_PROVIDER_CLASS =
      "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain";


  @Bean(name = "athenaDS")
  @ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
  public DataSource athenaDataSource(Environment env) {
    log.info("Create Athena datasource");

    DataSource ds = new com.simba.athena.jdbc.DataSource();
    ds.setCustomProperty("LogPath", env.getProperty("athena.log.path"));
    ds.setCustomProperty("LogLevel", env.getProperty("athena.log.level"));
    ds.setCustomProperty("S3OutputLocation", env.getProperty("athena.output.location"));
    ds.setCustomProperty("AwsCredentialsProviderClass", AWS_CREDENTIALS_PROVIDER_CLASS);
    ds.setCustomProperty("Workgroup", env.getProperty("athena.workgroup"));
    ds.setURL(env.getProperty("athena.url"));
    return ds;
  }


  @Autowired
  @Bean(name = "athenaJdbcTemplate")
  @ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
  public JdbcTemplate getJdbcTemplate(@Qualifier("athenaDS") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }


}

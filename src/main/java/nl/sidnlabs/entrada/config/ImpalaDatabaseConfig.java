package nl.sidnlabs.entrada.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import com.cloudera.impala.jdbc41.DataSource;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class ImpalaDatabaseConfig {


  @Bean(name = "impalaDS")
  @ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
  public DataSource athenaDataSource(Environment env) {
    log.info("Create Impala datasource");

    DataSource ds = new DataSource();
    // check to see if we are connection to a Keberized Impala
    if (!StringUtils.isBlank(env.getProperty("kerberos.keytab"))) {
      ds.setURL(env.getProperty("kerberos.impala.url"));
    } else {
      ds.setURL(env.getProperty("impala.url"));
    }
    return ds;
  }

  @Autowired
  @Bean(name = "impalaJdbcTemplate")
  @ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
  public JdbcTemplate getJdbcTemplate(@Qualifier("impalaDS") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }


}

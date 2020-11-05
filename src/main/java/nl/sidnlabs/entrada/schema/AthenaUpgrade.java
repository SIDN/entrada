package nl.sidnlabs.entrada.schema;

import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component("athenaUpgrade")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AthenaUpgrade extends AbstractUpgrade {

  // we need option so only 1 instance act as master instance to prevent
  // multiple instances running the db upgrade process
  @Value("${entrada.node.master:true}")
  private boolean enabled;

  public AthenaUpgrade(@Qualifier("athenaJdbcTemplate") JdbcTemplate jdbcTemplate) {
    super(jdbcTemplate, "athena");
  }

  @PostConstruct
  @Override
  public void upgrade() {
    if (!enabled) {
      log.info("Upgrade Athena database schema NOT enabled for this instance");
      return;
    }

    execute();
  }

}

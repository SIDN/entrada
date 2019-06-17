package nl.sidnlabs.entrada.engine;

import java.util.Set;
import java.util.concurrent.Future;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.model.Partition;

@Log4j2
@Component("athena")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AthenaQueryEngine implements QueryEngine {

  private static final String SQL_REPAIR_TABLE = "MSCK REPAIR TABLE ";


  @Value("${entrada.database.name}")
  private String database;


  private JdbcTemplate jdbcTemplate;

  public AthenaQueryEngine(@Qualifier("athenaJdbcTemplate") JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  @Async
  public Future<Boolean> execute(String sql) {
    try {
      jdbcTemplate.execute(sql);
    } catch (DataAccessException e) {
      log.error("SQL execution failed, sql: {}", sql, e);
      return new AsyncResult<>(Boolean.FALSE);
    }

    return new AsyncResult<>(Boolean.TRUE);
  }

  @Override
  @Async
  public Future<Boolean> addPartition(String table, Set<Partition> partitions, String location) {
    // The data is already in Hive format, no need to create individual partitions
    // just run "MSCK REPAIR TABLE" to have Athena discover then new partitions.
    return execute(SQL_REPAIR_TABLE + database + "." + table);
  }

}

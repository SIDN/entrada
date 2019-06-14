package nl.sidnlabs.entrada.engine;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
@Component("athena")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AthenaQueryEngine implements QueryEngine {

  private static final String SQL_ADD_PARTITION =
      "ALTER TABLE ${DATABASE_NAME}.${TABLE_NAME} ADD IF NOT EXISTS PARTITION (year=${YEAR},month=${MONTH},day=${DAY},server=${SERVER}) LOCATION='${S3_LOC}'";

  @Value("${entrada.database.name}")
  private String database;


  private JdbcTemplate jdbcTemplate;

  public AthenaQueryEngine(@Qualifier("athenaJdbcTemplate") JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  @Override
  public boolean executeSql(String sql) {
    try {
      jdbcTemplate.execute(sql);
    } catch (DataAccessException e) {
      log.error("SQL execution failed, sql: {}", sql, e);
      return false;
    }

    return true;
  }

  public boolean addPartition(String table, int year, int month, int day, String server,
      String location) {

    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", table);
    values.put("YEAR", Integer.valueOf(year));
    values.put("MONTH", Integer.valueOf(month));
    values.put("DAY", Integer.valueOf(day));
    values.put("SERVER", server);
    values.put("S3_LOC", location);

    String sql = TemplateUtil.template(SQL_ADD_PARTITION, values);
    log.info("Create partition, sql: {}", sql);

    return executeSql(sql);

  }

}

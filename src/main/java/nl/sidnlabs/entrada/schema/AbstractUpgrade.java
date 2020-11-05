package nl.sidnlabs.entrada.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
public abstract class AbstractUpgrade implements Upgrade {

  @Value("${entrada.database.name}")
  private String database;

  @Value("${entrada.database.table.dns}")
  protected String tableDns;

  @Value("${entrada.database.table.icmp}")
  protected String tableIcmp;

  protected JdbcTemplate jdbcTemplate;
  protected String prefix;

  public AbstractUpgrade(JdbcTemplate jdbcTemplate, String prefix) {
    this.jdbcTemplate = jdbcTemplate;
    this.prefix = prefix;
  }


  protected void execute() {
    // upgrade icmp
    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", tableIcmp);
    readStatements(values, "icmp").forEach(s -> runSql(s));

    // upgrade dns
    values.put("TABLE_NAME", tableDns);
    readStatements(values, "dns").forEach(s -> runSql(s));
  }

  private void runSql(String sql) {
    if (log.isDebugEnabled()) {
      log.info("Execute SQL: {}", sql);
    }

    try {
      jdbcTemplate.execute(sql);
      log.info("SQL execution OK");
    } catch (Exception e) {
      // Athena does not support "IF EXISTS" for adding columns
      // this will cause a duplicate column name exception
      // if the column already exists
      log.error("Database upgrade SQL DDL statement failed, possible duplicate column name");
      log.debug("Database exception: {}", e);
    }
  }

  protected List<String> readStatements(Map<String, Object> values, String table) {
    // TODO: for now only icmp is supported, add support for dns when required


    String stmts = TemplateUtil
        .readTemplate(
            new ClassPathResource("/sql/" + prefix + "/upgrade-" + table + ".sql", getClass()));

    return Arrays
        .stream(StrSubstitutor.replace(stmts, values).split("\\n"))
        .map(StringUtils::trimToEmpty)
        .filter(StringUtils::isNotBlank)
        .filter(s -> !s.startsWith("#"))
        .collect(Collectors.toList());
  }

}

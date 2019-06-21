package nl.sidnlabs.entrada.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
public abstract class AbstractQueryEngine implements QueryEngine {

  @Value("${entrada.location.output}")
  protected String outputLocation;

  @Value("${entrada.database.name}")
  private String database;

  protected JdbcTemplate jdbcTemplate;
  protected FileManager fileManager;
  private String scriptPrefix;


  public AbstractQueryEngine(JdbcTemplate jdbcTemplate, FileManager fileManager,
      String scriptPrefix) {
    this.jdbcTemplate = jdbcTemplate;
    this.fileManager = fileManager;
    this.scriptPrefix = scriptPrefix;
  }

  @Override
  public boolean execute(String sql) {
    if (log.isDebugEnabled()) {
      log.debug("Execute SQL: {}", sql);
    }

    try {
      jdbcTemplate.execute(sql);
    } catch (Exception e) {
      log.error("SQL execution failed, sql: {}", sql, e);
      return false;
    }

    return true;
  }

  @Override
  public boolean compact(TablePartition p) {
    String location = compactionLocation(p);

    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", p.getTable());
    values.put("TABLE_LOC", tableLocation(p));
    values.put("YEAR", p.getYear());
    values.put("MONTH", p.getMonth());
    values.put("DAY", p.getDay());
    values.put("SERVER", p.getServer());

    String dropTableSql =
        TemplateUtil.template("DROP TABLE IF EXISTS ${DATABASE_NAME}.tmp_compaction;", values);

    if (!cleanup(location, dropTableSql)) {
      return false;
    }

    String sql = TemplateUtil
        .template(
            new ClassPathResource("/sql/" + scriptPrefix + "/partition-compaction.sql", getClass()),
            values);

    // create tmp table with compacted parquet files AND delete old parquet files from src table
    if (!execute(sql) || !fileManager
        .delete(FileUtil.appendPath(outputLocation, p.getTable(), p.getPath()), true)) {
      return false;
    }

    // get list of compacted files
    List<String> filesToMove = fileManager.files(FileUtil.appendPath(location, p.getPath()));
    // move new parquet files to src table now.
    for (String f : filesToMove) {
      if (!move(f, p)) {
        return false;
      }
    }

    // cleanup
    return cleanup(location, dropTableSql);
  }

  private boolean move(String src, TablePartition p) {
    // create a new filename and encode the date and nameserver info in the filename
    String newName = FileUtil
        .appendPath(outputLocation, p.getTable(), p.getPath(),
            StringUtils
                .join(p.getYear(), StringUtils.leftPad(String.valueOf(p.getMonth()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(p.getDay()), 2, "0"))
                + "-" + p.getServer() + "-" + UUID.randomUUID() + ".parquet");

    return fileManager.move(src, newName, false);
  }

  private boolean cleanup(String prefix, String dropTableSql) {
    // delete any old data files on s3 and make sure the tmp table is not still around
    return fileManager.delete(prefix, true) && execute(dropTableSql);
  }

  // private boolean exec(String sql) {
  // try {
  // return execute(sql).get(5, TimeUnit.MINUTES).equals(Boolean.TRUE);
  // } catch (Exception e) {
  // log.error("Failed executing SQL: " + sql);
  // }
  //
  // return false;
  // }


}

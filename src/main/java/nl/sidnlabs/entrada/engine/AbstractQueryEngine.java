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

  private static final String SQL_DROP_TMP_TABLE =
      "DROP TABLE IF EXISTS ${DATABASE_NAME}.tmp_compaction;";

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

    String dropTableSql = TemplateUtil.template(SQL_DROP_TMP_TABLE, values);

    if (!cleanup(location, dropTableSql)) {
      return false;
    }

    String sql = TemplateUtil.template(getCompactionScript(p.getTable()), values);

    // create tmp table with compacted parquet files AND delete old parquet files from src table
    if (!(execute(sql)
        && deleteSrcParquetData(fileManager,
            FileUtil.appendPath(outputLocation, p.getTable(), p.getPath()))
        && move(p, FileUtil.appendPath(location, p.getPath())))) {
      return false;
    }

    // perform post-compact actions and do cleanup
    return postCompact(p) && cleanup(location, dropTableSql);
  }

  private ClassPathResource getCompactionScript(String table) {
    return new ClassPathResource("/sql/" + scriptPrefix + "/partition-compaction-" + table + ".sql",
        getClass());
  }

  private boolean deleteSrcParquetData(FileManager fileManager, String location) {
    List<String> files = fileManager.files(location);
    for (String f : files) {
      if (StringUtils.endsWith(f, ".parquet") && !fileManager.delete(f)) {
        return false;
      }
    }
    return true;
  }

  private boolean move(TablePartition p, String location) {
    // get list of compacted files
    List<String> filesToMove = fileManager.files(location);
    // move new parquet files to src table now.
    for (String f : filesToMove) {

      // create a new filename and encode the date and server name in the filename
      String newName = FileUtil
          .appendPath(outputLocation, p.getTable(), p.getPath(),
              StringUtils
                  .join(p.getYear(), StringUtils.leftPad(String.valueOf(p.getMonth()), 2, "0"),
                      StringUtils.leftPad(String.valueOf(p.getDay()), 2, "0"))
                  + "-" + p.getServer() + "-" + UUID.randomUUID() + ".parquet");

      if (!fileManager.move(f, newName, false)) {
        return false;
      }
    }
    return true;
  }

  private boolean cleanup(String location, String dropTableSql) {
    // delete any old data and make sure the tmp table is not still around
    return fileManager.rmdir(location) && execute(dropTableSql);
  }

}

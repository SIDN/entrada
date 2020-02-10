package nl.sidnlabs.entrada.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
public abstract class AbstractQueryEngine implements QueryEngine {

  private static final String SQL_DROP_TMP_TABLE =
      "DROP TABLE IF EXISTS ${DATABASE_NAME}.tmp_compaction;";

  private static final String PARQUET_FILE_EXT = ".parq";

  @Value("${entrada.location.output}")
  protected String outputLocation;

  @Value("${entrada.database.name}")
  private String database;

  @Value("${entrada.database.table.dns}")
  protected String tableDns;

  @Value("${entrada.database.table.icmp}")
  protected String tableIcmp;

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

    Map<String, Object> values = createValueMap(p);

    String dropTableSql = TemplateUtil.template(SQL_DROP_TMP_TABLE, values);

    if (!cleanup(location, dropTableSql)) {
      log.error("Cannot cleanup compaction resources, cannot continue with compaction");
      return false;
    }

    String sql = TemplateUtil.template(getCompactionScript(p.getTable()), values);

    List<String> sourceFiles =
        listSourceParquetData(FileUtil.appendPath(outputLocation, p.getTable(), p.getPath()));

    // create tmp table and select all data into this table, this will compacted all the data in new
    // larger parquet files
    // then move the new files to the src table AND delete the old parquet files from src table
    if (!preCompact(p) || !execute(sql)) {
      log
          .error("Compaction for table: {} failed, could not create new parquet files",
              p.getTable());
      return false;
    }

    List<String> filesToMove = listSourceParquetData(FileUtil.appendPath(location, p.getPath()));
    if (!move(p, filesToMove)) {
      log.error("Compaction for table: {} failed, could not move new parquet data", p.getTable());
      // rollback moving data files, by deleting parquet files that have already been moved to the
      // new location, ignore any error from deleteFiles method, because some of the files might not
      // exist
      deleteFiles(newFilesToDelete(p, outputLocation, filesToMove));
      return false;
    }

    int deleteErrors = deleteFiles(sourceFiles);
    if (deleteErrors > 0) {
      log
          .error("Compaction for table: {} failed, could not delete {} source parquet file(s)",
              deleteErrors, p.getTable());
      return false;
    }

    // perform post-compact actions and do cleanup
    return postCompact(p) && cleanup(location, dropTableSql);
  }

  private Map<String, Object> createValueMap(TablePartition p) {
    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    switch(p.getTable()) {
      case "dns":
        values.put("TABLE_NAME", tableDns);
        break;
      case "icmp":
        values.put("TABLE_NAME", tableIcmp);
        break;
    }
    values.put("TABLE_LOC", tableLocation(p));
    values.put("YEAR", p.getYear());
    values.put("MONTH", p.getMonth());
    values.put("DAY", p.getDay());
    values.put("SERVER", p.getServer());
    return values;
  }

  private List<String> newFilesToDelete(TablePartition p, String newlocation, List<String> files) {
    List<String> newFilesToDelete = new ArrayList<>();
    for (String f : files) {
      String newName = FileUtil
          .appendPath(newlocation, p.getTable(), p.getPath(),
              StringUtils.substringAfterLast(f, "/"));
      newFilesToDelete.add(newName);
    }
    return newFilesToDelete;
  }

  private ClassPathResource getCompactionScript(String table) {
    return new ClassPathResource("/sql/" + scriptPrefix + "/partition-compaction-" + table + ".sql",
        getClass());
  }

  private int deleteFiles(List<String> files) {
    int errors = 0;
    for (String f : files) {
      if (!fileManager.delete(f)) {
        errors++;
      }
    }
    return errors;
  }

  private List<String> listSourceParquetData(String location) {
    return fileManager.files(location).stream().collect(Collectors.toList());
  }

  private boolean move(TablePartition p, List<String> files) {
    // move new parquet files to src table.

    // create server component of filename, skip if server is null ( can be for icmp)
    String svr = p.getServer() != null ? "-" + p.getServer() + "-" : "";

    for (String f : files) {

      // create a new filename and encode the date and server name in the filename
      String newName = FileUtil
          .appendPath(outputLocation, p.getTable(), p.getPath(),
              StringUtils
                  .join(p.getYear(), StringUtils.leftPad(String.valueOf(p.getMonth()), 2, "0"),
                      StringUtils.leftPad(String.valueOf(p.getDay()), 2, "0"))
                  + svr + UUID.randomUUID() + PARQUET_FILE_EXT);

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


  @Override
  public boolean addPartition(String type, String table, Set<Partition> partitions) {

    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", table);

    for (Partition p : partitions) {
      log.info("Add partition: {} to table: {}", p, table);

      values.put("YEAR", Integer.valueOf(p.getYear()));
      values.put("MONTH", Integer.valueOf(p.getMonth()));
      values.put("DAY", Integer.valueOf(p.getDay()));
      values.put("SERVER", p.getServer());

      String sql = TemplateUtil
          .template(new ClassPathResource("/sql/create-partition-" + type + ".sql", getClass()),
              values);

      log.info("Create partition, sql: {}", sql);

      if (!execute(sql) || !postAddPartition(table, p)) {
        log.error("Create partition failed for: {}", p);
        return false;
      }
    }

    return true;
  }

  public boolean postCompact(TablePartition p) {
    // do nothing
    return true;
  }

  public boolean preCompact(TablePartition p) {
    // do nothing
    return true;
  }

  @Override
  public boolean purge(TablePartition p) {
    String location = compactionLocation(p);

    Map<String, Object> values = createValueMap(p);

    // reuse the tmp_compaction also for purging, the scheduled job
    // must make sure compacxtion and purging will never run at the same time.
    String dropTableSql = TemplateUtil.template(SQL_DROP_TMP_TABLE, values);

    if (!cleanup(location, dropTableSql)) {
      log.error("Cannot cleanup purging resources, cannot continue with purging");
      return false;
    }

    String sql = TemplateUtil.template(getPurgeScript(p.getTable()), values);

    List<String> sourceFiles =
        listSourceParquetData(FileUtil.appendPath(outputLocation, p.getTable(), p.getPath()));

    // create tmp table and select all data into this table, this will purge all PII attributes and
    // write all the data in new parquet files then move the new files to the src table AND
    // delete the old parquet files from the src table
    if (!execute(sql)) {
      log.error("Purging for table: {} failed, could not create new parquet files", p.getTable());
      return false;
    }

    // purge create new data files, get list of old files and move new files to correct location
    // and delete the old files.
    List<String> filesToMove = listSourceParquetData(FileUtil.appendPath(location, p.getPath()));
    if (!move(p, filesToMove)) {
      log.error("Compaction for table: {} failed, could not move new parquet data", p.getTable());
      // rollback moving data files, by deleting parquet files that have already been moved to the
      // new location, ignore any error from deleteFiles method, because some of the files might not
      // exist
      deleteFiles(newFilesToDelete(p, outputLocation, filesToMove));
      return false;
    }

    int deleteErrors = deleteFiles(sourceFiles);
    if (deleteErrors > 0) {
      log
          .error("Purging for table: {} failed, could not delete {} source parquet file(s)",
              deleteErrors, p.getTable());
      return false;
    }

    // perform post-purge actions and do cleanup
    return postPurge(p) && cleanup(location, dropTableSql);
  }

  private ClassPathResource getPurgeScript(String table) {
    return new ClassPathResource("/sql/" + scriptPrefix + "/partition-purge-" + table + ".sql",
        getClass());
  }

  @Override
  public boolean postPurge(TablePartition p) {
    // do nothing
    return true;
  }


}

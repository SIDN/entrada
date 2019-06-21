package nl.sidnlabs.entrada.engine;

import java.util.Set;
import java.util.concurrent.Future;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.util.FileUtil;

@Component("athena")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AthenaQueryEngine extends AbstractQueryEngine {

  private static final String SQL_REPAIR_TABLE = "MSCK REPAIR TABLE ";

  @Value("${entrada.location.output}")
  private String outputLocation;

  @Value("${entrada.database.name}")
  private String database;

  @Value("${cloud.aws.bucket}")
  private String bucket;


  public AthenaQueryEngine(@Qualifier("athenaJdbcTemplate") JdbcTemplate jdbcTemplate,
      @Qualifier("s3") FileManager fileManager) {
    super(jdbcTemplate, fileManager, "athena");
  }

  // @Override
  // @Async
  // public Future<Boolean> execute(String sql) {
  // if (log.isDebugEnabled()) {
  // log.debug("Execute SQL: {}", sql);
  // }
  //
  // try {
  // jdbcTemplate.execute(sql);
  // } catch (DataAccessException e) {
  // log.error("SQL execution failed, sql: {}", sql, e);
  // return new AsyncResult<>(Boolean.FALSE);
  // }
  //
  // return new AsyncResult<>(Boolean.TRUE);
  // }

  @Override
  @Async
  public Future<Boolean> addPartition(String table, Set<Partition> partitions, String location) {
    // The data is already in Hive format, no need to create individual partitions
    // just run "MSCK REPAIR TABLE" to have Athena discover then new partitions.
    if (execute(SQL_REPAIR_TABLE + database + "." + table)) {
      return new AsyncResult<>(Boolean.TRUE);
    }

    return new AsyncResult<>(Boolean.FALSE);
  }

  @Override
  public String compactionLocation(TablePartition p) {
    return FileUtil.appendPath("s3://" + bucket, "tmp_compaction/" + p.getTable());
  }


  @Override
  public String tableLocation(TablePartition p) {
    return FileUtil.appendPath(compactionLocation(p), p.getPath());
  }


  // @Override
  // public boolean compact(TablePartition p) {
  // String prefix = FileUtil.appendPath("s3://" + bucket, "tmp_compaction/" + p.getTable() + "/");
  //
  // Map<String, Object> values = new HashMap<>();
  // values.put("DATABASE_NAME", database);
  // values.put("TABLE_NAME", p.getTable());
  // values.put("TABLE_LOC", prefix + p.getPath());
  // values.put("YEAR", p.getYear());
  // values.put("MONTH", p.getMonth());
  // values.put("DAY", p.getDay());
  // values.put("SERVER", p.getServer());
  //
  // String dropTableSql =
  // TemplateUtil.template("DROP TABLE IF EXISTS ${DATABASE_NAME}.tmp_compaction;", values);
  //
  // if (!cleanup(prefix, dropTableSql)) {
  // return false;
  // }
  //
  // String sql = TemplateUtil
  // .template(new ClassPathResource("/sql/athena/partition-compaction.sql", getClass()),
  // values);
  //
  // // create tmp table with compacted parquet files AND delete old parquet files from src table
  // if (!exec(sql) || !fileManager
  // .delete(FileUtil.appendPath(outputLocation, p.getTable(), p.getPath()), true)) {
  // return false;
  // }
  //
  // // get list of compacted files
  // List<String> filesToMove = fileManager.files(prefix);
  // // move new parquet files to src table now.
  // for (String f : filesToMove) {
  // if (!move(f, p)) {
  // return false;
  // }
  // }
  //
  // // cleanup
  // return cleanup(prefix, dropTableSql);
  // }

  // private boolean move(String src, TablePartition p) {
  // // create a new filename and encode the date and nameserver info in the filename
  // String newName = FileUtil
  // .appendPath(outputLocation, p.getTable(), p.getPath(),
  // StringUtils
  // .join(p.getYear(), StringUtils.leftPad(String.valueOf(p.getMonth()), 2, "0"),
  // StringUtils.leftPad(String.valueOf(p.getDay()), 2, "0"))
  // + "-" + p.getServer() + "-" + UUID.randomUUID() + ".parquet");
  //
  // return fileManager.move(src, newName, false);
  // }

  // private boolean cleanup(String prefix, String dropTableSql) {
  // // delete any old data files on s3 and make sure the tmp table is not still around
  // return fileManager.delete(prefix, true) && exec(dropTableSql);
  // }

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

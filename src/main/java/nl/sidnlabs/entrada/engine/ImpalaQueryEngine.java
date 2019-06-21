package nl.sidnlabs.entrada.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
@Component("impala")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
public class ImpalaQueryEngine extends AbstractQueryEngine {

  private static final String SQL_PARTITION_TEMPLATE =
      "(year=${YEAR},month=${MONTH},day=${DAY},server='${SERVER}')";

  private static final String SQL_REFRESH_TABLE =
      "REFRESH ${DATABASE_NAME}.${TABLE_NAME} PARTITION " + SQL_PARTITION_TEMPLATE;


  @Value("${entrada.database.name}")
  private String database;

  @Value("${hdfs.nameservice}")
  private String hdfsNameservice;


  public ImpalaQueryEngine(@Qualifier("impalaJdbcTemplate") JdbcTemplate jdbcTemplate,
      @Qualifier("hdfs") FileManager fileManager) {
    super(jdbcTemplate, fileManager, "impala");
  }

  // @Override
  // @Async
  // public Future<Boolean> execute(String sql) {
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

    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", table);

    StringBuilder pSql = new StringBuilder();
    for (Partition p : partitions) {
      values.put("YEAR", Integer.valueOf(p.getYear()));
      values.put("MONTH", Integer.valueOf(p.getMonth()));
      values.put("DAY", Integer.valueOf(p.getDay()));
      values.put("SERVER", p.getServer());

      pSql.append(TemplateUtil.template(SQL_PARTITION_TEMPLATE, values));
    }

    values.put("PARTITION", pSql);
    String sql = TemplateUtil
        .template(new ClassPathResource("/sql/impala/create-partition.sql", getClass()), values);

    log.info("Create partition, sql: {}", sql);

    // do a refresh after the partition has been added to update the table metadata
    String sqlRefresh = TemplateUtil.template(SQL_REFRESH_TABLE, values);

    if (execute(sql) && execute(sqlRefresh)) {
      return new AsyncResult<>(Boolean.TRUE);
    }
    return new AsyncResult<>(Boolean.FALSE);
  }



  @Override
  public String compactionLocation(TablePartition p) {
    return FileUtil
        .appendPath(StringUtils.substringAfter(outputLocation, hdfsNameservice),
            "tmp_compaction/" + p.getTable());
  }

  @Override
  public String tableLocation(TablePartition p) {
    return compactionLocation(p);
  }



}

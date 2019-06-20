package nl.sidnlabs.entrada.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
@Component("impala")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
public class ImpalaQueryEngine implements QueryEngine {

  private static final String SQL_PARTITION_TEMPLATE =
      "(year=${YEAR},month=${MONTH},day=${DAY}) LOCATION='${S3_LOC}'";
  private static final String SQL_PARTITION_SVR_TEMPLATE =
      "(year=${YEAR},month=${MONTH},day=${DAY},server='${SERVER}') LOCATION='${S3_LOC}'";


  @Value("${entrada.database.name}")
  private String database;


  private JdbcTemplate jdbcTemplate;

  public ImpalaQueryEngine(@Qualifier("impalaJdbcTemplate") JdbcTemplate jdbcTemplate) {
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

    String partitionsStr = partitions
        .stream()
        .map(p -> createPartitionStmt(p, location))
        .collect(Collectors.joining("\n"));


    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", table);
    values.put("PARTITIONS", partitionsStr);

    String sql = TemplateUtil
        .template(new ClassPathResource("/sql/impala/create-partition.sql", getClass()), values);

    log.info("Create partition, sql: {}", sql);

    return execute(sql);

  }

  private String createPartitionStmt(Partition partition, String location) {

    Map<String, Object> values = new HashMap<>();
    values.put("YEAR", Integer.valueOf(partition.getYear()));
    values.put("MONTH", Integer.valueOf(partition.getMonth()));
    values.put("DAY", Integer.valueOf(partition.getDay()));
    values.put("S3_LOC", FileUtil.appendPath(location, partition.toPath()));

    if (StringUtils.isNotBlank(partition.getServer())) {
      values.put("SERVER", partition.getServer());
      return TemplateUtil.template(SQL_PARTITION_SVR_TEMPLATE, values);
    }
    return TemplateUtil.template(SQL_PARTITION_TEMPLATE, values);
  }


  @Override
  public boolean compact(TablePartition p) {
    // TODO Auto-generated method stub
    return false;
  }

}

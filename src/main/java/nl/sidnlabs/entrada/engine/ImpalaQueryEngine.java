package nl.sidnlabs.entrada.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
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

  private static final String SQL_COMPUTE_STATS =
      "COMPUTE INCREMENTAL STATS ${DATABASE_NAME}.${TABLE_NAME} PARTITION "
          + SQL_PARTITION_TEMPLATE;


  @Value("${entrada.database.name}")
  private String database;

  @Value("${hdfs.nameservice}")
  private String hdfsNameservice;


  public ImpalaQueryEngine(@Qualifier("impalaJdbcTemplate") JdbcTemplate jdbcTemplate,
      @Qualifier("hdfs") FileManager fileManager) {
    super(jdbcTemplate, fileManager, "impala");
  }

  @Override
  public boolean addPartition(String table, Set<Partition> partitions) {

    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", table);

    for (Partition p : partitions) {
      log.info("Add partition: {} to table: {}", p, table);

      values.put("YEAR", Integer.valueOf(p.getYear()));
      values.put("MONTH", Integer.valueOf(p.getMonth()));
      values.put("DAY", Integer.valueOf(p.getDay()));
      values.put("SERVER", p.getServer());
      values.put("PARTITION", TemplateUtil.template(SQL_PARTITION_TEMPLATE, values));

      String sql = TemplateUtil
          .template(new ClassPathResource("/sql/impala/create-partition.sql", getClass()), values);

      log.info("Create partition, sql: {}", sql);

      if (!execute(sql)) {
        return false;
      }


      // do a refresh after the partition has been added to update the table metadata
      String sqlRefresh = TemplateUtil.template(SQL_REFRESH_TABLE, values);
      if (!execute(sqlRefresh)) {
        return false;
      }
    }

    return true;
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

  @Override
  public boolean postCompact(TablePartition p) {
    log.info("Perform post-compaction actions");

    log.info("Execute refresh and compute stats for table: {}", p.getTable());
    // update meta data
    return execute(TemplateUtil.template(SQL_REFRESH_TABLE, templateValues(p)));
  }


  private Map<String, Object> templateValues(TablePartition p) {
    log.info("Perform post-compaction actions");

    log.info("Execute refresh and compute stats for table: {}", p.getTable());
    // update meta data and compute stats for partition
    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", p.getTable());
    values.put("YEAR", Integer.valueOf(p.getYear()));
    values.put("MONTH", Integer.valueOf(p.getMonth()));
    values.put("DAY", Integer.valueOf(p.getDay()));
    values.put("SERVER", p.getServer());
    values.put("PARTITION", TemplateUtil.template(SQL_PARTITION_TEMPLATE, values));

    return values;
  }

  @Override
  public boolean preCompact(TablePartition p) {
    // update partition stats before compaction so the query planner can optimize the CTAS query
    return execute(TemplateUtil.template(SQL_COMPUTE_STATS, templateValues(p)));
  }

}

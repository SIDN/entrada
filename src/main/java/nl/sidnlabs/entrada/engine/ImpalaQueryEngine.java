package nl.sidnlabs.entrada.engine;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
@Component("impala")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "hadoop")
public class ImpalaQueryEngine extends AbstractQueryEngine {

  @Value("${entrada.database.name}")
  private String database;

  @Value("${hdfs.nameservice}")
  private String hdfsNameservice;


  public ImpalaQueryEngine(@Qualifier("impalaJdbcTemplate") JdbcTemplate jdbcTemplate,
      @Qualifier("hdfs") FileManager fileManager) {
    super(jdbcTemplate, fileManager, "impala");
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

    log
        .info("Perform post-compaction actions, refresh and compute stats for table: {}",
            p.getTable());
    Map<String, Object> values = templateValues(p);
    // update meta data to let impala know the files have been updated and recalculate partition
    // statistics

    String sqlRefresh = TemplateUtil
        .template(new ClassPathResource("/sql/impala/refresh-partition-" + p.getTable() + ".sql",
            getClass()), values);

    String sqlComputeStats = TemplateUtil
        .template(
            new ClassPathResource("/sql/impala/compute-stats-" + p.getTable() + ".sql", getClass()),
            values);

    return execute(sqlRefresh) && execute(sqlComputeStats);
  }


  private Map<String, Object> templateValues(TablePartition p) {
    Map<String, Object> values = new HashMap<>();
    values.put("DATABASE_NAME", database);
    values.put("TABLE_NAME", p.getTable());
    values.put("YEAR", Integer.valueOf(p.getYear()));
    values.put("MONTH", Integer.valueOf(p.getMonth()));
    values.put("DAY", Integer.valueOf(p.getDay()));
    values.put("SERVER", p.getServer());
    return values;
  }

  @Override
  public boolean preCompact(TablePartition p) {
    // update partition stats before compaction so the query planner can optimize the CTAS query
    String sqlComputeStats = TemplateUtil
        .template(
            new ClassPathResource("/sql/impala/compute-stats-" + p.getTable() + ".sql", getClass()),
            templateValues(p));

    return execute(sqlComputeStats);
  }

}

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

  @Value("${entrada.database.name}")
  private String database;

  @Value("${aws.bucket}")
  private String bucket;


  public AthenaQueryEngine(@Qualifier("athenaJdbcTemplate") JdbcTemplate jdbcTemplate,
      @Qualifier("s3") FileManager fileManager) {
    super(jdbcTemplate, fileManager, "athena");
  }

  @Override
  @Async
  public Future<Boolean> addPartition(String table, Set<Partition> partitions) {
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

  @Override
  public boolean postCompact(String table, TablePartition p) {
    // update meta data
    return execute(SQL_REPAIR_TABLE + database + "." + table);
  }

}

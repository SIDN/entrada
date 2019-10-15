package nl.sidnlabs.entrada.engine;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.jpa.TablePartition;
import nl.sidnlabs.entrada.util.FileUtil;

@Component("athena")
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AthenaQueryEngine extends AbstractQueryEngine {

  @Value("${entrada.database.name}")
  private String database;

  @Value("${aws.bucket}")
  private String bucket;


  public AthenaQueryEngine(@Qualifier("athenaJdbcTemplate") JdbcTemplate jdbcTemplate,
      @Qualifier("s3") FileManager fileManager) {
    super(jdbcTemplate, fileManager, "athena");
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
  public boolean postAddPartition(String table, Partition p) {
    // do nothing
    return true;
  }

}

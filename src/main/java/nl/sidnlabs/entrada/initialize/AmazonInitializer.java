package nl.sidnlabs.entrada.initialize;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AmazonInitializer implements Initializer {

  @Value("${cloud.aws.bucket}")
  private String bucket;

  @Value("${entrada.database.name}")
  private String database;

  @Value("${entrada.database.table.dns}")
  private String tableDns;

  @Value("${entrada.database.table.icmp}")
  private String tableIcmp;

  @Value("${entrada.location.output}")
  private String outputLocation;

  @Value("${entrada.icmp.enable}")
  private boolean icmpEnabled;

  private QueryEngine queryEngine;
  private AmazonS3 amazonS3;
  private FileManager fileManager;

  public AmazonInitializer(AmazonS3 amazonS3, @Qualifier("s3") FileManager fileManager,
      @Qualifier("athena") QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
    this.amazonS3 = amazonS3;
    this.fileManager = fileManager;
  }

  @PostConstruct
  public void init() {
    log.info("Perform AWS provisioning");
    try {
      createStorage();
    } catch (Exception e) {
      throw new ApplicationException("Error while creating s3 bucket", e);
    }

    createDatabase();
  }

  @Override
  public void createStorage() {
    // check if the s3 bucket and required directories exist and if not create these
    if (!amazonS3.doesBucketExistV2(bucket)) {
      amazonS3.createBucket(bucket);

      // make sure to block all public access to the bucket
      amazonS3
          .setPublicAccessBlock(new SetPublicAccessBlockRequest()
              .withBucketName(bucket)
              .withPublicAccessBlockConfiguration(new PublicAccessBlockConfiguration()
                  .withBlockPublicAcls(Boolean.TRUE)
                  .withIgnorePublicAcls(Boolean.TRUE)
                  .withBlockPublicPolicy(Boolean.TRUE)
                  .withRestrictPublicBuckets(Boolean.TRUE)));

    }

    /*
     * no need to check if directories are present. in S3 there are no directories, just prefixes
     * that are set on a file when it is uploaded.
     */
  }

  @Override
  public void createDatabase() {
    // create the athena database schema
    if (!fileManager.supported(outputLocation)) {
      throw new ApplicationException(
          "Selected mode is AWS but the ENTRADA output location does not use S3, cannot continue");
    }

    // create database
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME", database);
    parameters.put("S3_LOC", outputLocation);

    String sql = TemplateUtil
        .template(
            new ClassPathResource("/sql/athena/create-database.sql", TemplateUtil.class.getClass()),
            parameters);
    executeSQL(sql);

    // create dns table
    parameters.put("TABLE_NAME", tableDns);
    parameters.put("S3_LOC", FileUtil.appendPath(outputLocation, tableDns));
    sql = TemplateUtil
        .template(new ClassPathResource("/sql/athena/create-table-dns.sql",
            TemplateUtil.class.getClass()), parameters);
    executeSQL(sql);

    // create icmp table
    if (icmpEnabled) {
      // create dns table
      parameters.put("TABLE_NAME", tableIcmp);
      parameters.put("S3_LOC", FileUtil.appendPath(outputLocation, tableIcmp));
      sql = TemplateUtil
          .template(new ClassPathResource("/sql/athena/create-table-icmp.sql",
              TemplateUtil.class.getClass()), parameters);
      executeSQL(sql);
    }
  }

  private void executeSQL(String sql) {
    try {
      if (queryEngine.execute(sql).get(5, TimeUnit.MINUTES).equals(Boolean.FALSE)) {
        // failed to execute sql
        throw new ApplicationException("Query failed");
      }
    } catch (Exception e) {
      throw new ApplicationException("Query failed");
    }
  }



}

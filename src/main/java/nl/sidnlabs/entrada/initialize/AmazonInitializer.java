package nl.sidnlabs.entrada.initialize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.GetBucketEncryptionResult;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.S3FileManagerImpl.S3Details;

@Log4j2
@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AmazonInitializer extends AbstractInitializer {

  @Value("${aws.bucket}")
  private String bucket;
  @Value("${athena.output.expiration}")
  private int expiration;
  @Value("${athena.output.location}")
  private String athenaOutputLocation;



  private AmazonS3 amazonS3;
  private FileManager fileManager;

  public AmazonInitializer(AmazonS3 amazonS3, @Qualifier("s3") FileManager fileManager,
      @Qualifier("athena") QueryEngine queryEngine) {

    super(queryEngine, "athena");
    this.amazonS3 = amazonS3;
    this.fileManager = fileManager;
  }

  @Override
  public boolean initializeStorage() {
    if (!fileManager.supported(outputLocation)) {
      throw new ApplicationException(
          "Selected mode is AWS but the ENTRADA output location does not use S3, cannot continue");
    }

    // check if the s3 bucket and required directories exist and if not create these
    if (!amazonS3.doesBucketExistV2(bucket)) {
      log.info("Create bucket: " + bucket);

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

      // check to see if we should enable default encryption
      // and make sure that Athena results are deleted after automatically
      return enableEncryption() && enableBucketLifecycle();
    }

    /*
     * no need to check if directories are present. in S3 there are no directories, just prefixes
     * that are set on a file when it is uploaded.
     */
    return amazonS3.doesBucketExistV2(bucket);
  }

  private boolean enableBucketLifecycle() {
    Optional<S3Details> s3Loc = S3Details.from(athenaOutputLocation);
    if (s3Loc.isPresent()) {

      // create lifecycle policy for Athena results
      BucketLifecycleConfiguration.Rule rule = new BucketLifecycleConfiguration.Rule()
          .withId("Delete Athena results after " + expiration + " day(s)")
          .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(s3Loc.get().getKey())))
          .withExpirationInDays(expiration)
          .withStatus(BucketLifecycleConfiguration.ENABLED);

      BucketLifecycleConfiguration configuration =
          new BucketLifecycleConfiguration().withRules(Arrays.asList(rule));

      // Save the configuration.
      amazonS3.setBucketLifecycleConfiguration(bucket, configuration);

      return true;
    }

    return false;
  }

  private boolean enableEncryption() {

    if (!encrypt) {
      return true;
    }

    ServerSideEncryptionRule serverSideEncryptionRule = new ServerSideEncryptionRule();

    ServerSideEncryptionByDefault serverSideEncryptionByDefault =
        new ServerSideEncryptionByDefault();

    serverSideEncryptionByDefault.setSSEAlgorithm(SSEAlgorithm.AES256.getAlgorithm());

    serverSideEncryptionRule.setApplyServerSideEncryptionByDefault(serverSideEncryptionByDefault);

    SetBucketEncryptionRequest setBucketEncryptionRequest = new SetBucketEncryptionRequest();
    setBucketEncryptionRequest.setBucketName(bucket);

    ServerSideEncryptionConfiguration serverSideEncryptionConfiguration =
        new ServerSideEncryptionConfiguration();

    ArrayList<ServerSideEncryptionRule> serverSideEncryptionRules = new ArrayList<>();
    serverSideEncryptionRules.add(serverSideEncryptionRule);
    serverSideEncryptionConfiguration.setRules(serverSideEncryptionRules);

    setBucketEncryptionRequest
        .setServerSideEncryptionConfiguration(serverSideEncryptionConfiguration);

    amazonS3.setBucketEncryption(setBucketEncryptionRequest);

    GetBucketEncryptionResult result = amazonS3.getBucketEncryption(bucket);
    return !result.getServerSideEncryptionConfiguration().getRules().isEmpty();
  }

}

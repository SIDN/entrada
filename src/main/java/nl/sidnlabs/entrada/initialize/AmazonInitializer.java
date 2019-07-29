package nl.sidnlabs.entrada.initialize;

import java.util.ArrayList;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.BucketNameUtils;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
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
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.file.S3FileManagerImpl.S3Details;

@Log4j2
@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AmazonInitializer extends AbstractInitializer {

  @Value("${aws.bucket}")
  private String bucket;
  @Value("${athena.output.expiration}")
  private int outputExpiration;
  @Value("${athena.output.location}")
  private String athenaOutputLocation;

  @Value("${entrada.location.archive}")
  private String archiveLocation;
  @Value("${entrada.archive.files.max.age}")
  private int archiveExpiration;


  @Value("${aws.bucket.rules.url}")
  private String bucketRules;


  private AmazonS3 amazonS3;
  private FileManager fileManager;

  public AmazonInitializer(AmazonS3 amazonS3, @Qualifier("s3") FileManager fileManager,
      @Qualifier("athena") QueryEngine queryEngine, FileManagerFactory fileManagerFactory) {

    super(queryEngine, "athena", fileManagerFactory);
    this.amazonS3 = amazonS3;
    this.fileManager = fileManager;
  }

  @Override
  public boolean initializeStorage() {
    log.info("Provision AWS storage");

    // create local storage locations
    super.initializeStorage();

    if (!fileManager.supported(output)) {
      throw new ApplicationException(
          "Selected mode is AWS but the ENTRADA output location does not use S3, cannot continue");
    }

    // check if the s3 bucket and required directories exist and if not create these
    if (!BucketNameUtils.isValidV2BucketName(bucket)) {
      throw new ApplicationException("\"" + bucket
          + "\" is not a valid S3 bucket name, for bucket restrictions and limitations, see: "
          + bucketRules);
    }

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

      enableEncryption();
    }

    // check to see if we should enable default encryption
    // and make sure that Athena results are deleted after automatically
    // also make sure pcap files archived on S3 have a expiration lifecycle policy
    return amazonS3.doesBucketExistV2(bucket)
        && enableBucketLifecycle(athenaOutputLocation, "Delete Athena results", outputExpiration,
            false)
        && enableBucketLifecycle(archive, "Delete archived pcap-files", archiveExpiration, true);
  }

  private boolean enableBucketLifecycle(String location, String prefix, int exp, boolean optional) {

    if (optional && !fileManager.supported(location)) {
      // location not a s3 location, but it is an optional policy so not a problem
      // do nothing
      return true;
    }

    Optional<S3Details> s3Loc = S3Details.from(location);
    if (s3Loc.isPresent()) {

      BucketLifecycleConfiguration cfg = amazonS3.getBucketLifecycleConfiguration(bucket);
      if (cfg == null) {
        // no config found, ceate a new config
        cfg = new BucketLifecycleConfiguration().withRules(new ArrayList<>());
      }
      Optional<Rule> oldRule = cfg
          .getRules()
          .stream()
          .filter(r -> StringUtils.startsWithIgnoreCase(r.getId(), prefix))
          .findFirst();

      if (!oldRule.isPresent()) {
        // no rule found found create new
        cfg
            .getRules()
            .add(createExpirationRule(prefix + " after " + exp + " day(s)", s3Loc.get().getKey(),
                exp));
        // Save the configuration.
        amazonS3.setBucketLifecycleConfiguration(bucket, cfg);
        return true;
      } else {
        // existing rule found, check if need to update
        if (oldRule.get().getExpirationInDays() != exp) {
          log
              .info(
                  "Lifecycle policy rule with prefix: '{}' has changed from {} to {}, creating new policy rule.",
                  prefix, oldRule.get().getExpirationInDays(), exp);
          // remove old rule and add new rule
          cfg.getRules().remove(oldRule.get());
          cfg
              .getRules()
              .add(createExpirationRule(prefix + " after " + exp + " day(s)", s3Loc.get().getKey(),
                  exp));

          amazonS3.setBucketLifecycleConfiguration(bucket, cfg);
        }
        return true;
      }
    }
    return false;
  }

  private BucketLifecycleConfiguration.Rule createExpirationRule(String id, String prefix,
      int expiration) {
    return new BucketLifecycleConfiguration.Rule()
        .withId(id)
        .withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(prefix)))
        .withExpirationInDays(expiration)
        .withStatus(BucketLifecycleConfiguration.ENABLED);
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

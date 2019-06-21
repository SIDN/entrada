package nl.sidnlabs.entrada.initialize;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;

@Component
@ConditionalOnProperty(name = "entrada.engine", havingValue = "aws")
public class AmazonInitializer extends AbstractInitializer {

  @Value("${cloud.aws.bucket}")
  private String bucket;

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
    return true;
  }

}

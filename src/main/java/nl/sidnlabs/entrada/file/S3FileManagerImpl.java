package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class S3FileManagerImpl implements FileManager {

  private static final String S3_SCHEMA = "s3://";
  private AmazonS3 amazonS3;
  private TransferManager transferManager;

  public S3FileManagerImpl(AmazonS3 amazonS3,
      @org.springframework.beans.factory.annotation.Value("${cloud.aws.upload.multipart.mb.size}") int multipartSize) {
    this.amazonS3 = amazonS3;

    transferManager = TransferManagerBuilder
        .standard()
        .withS3Client(amazonS3)
        .withMultipartUploadThreshold(multipartSize * 1024L * 1024L)
        .build();
  }

  @Override
  public String schema() {
    return S3_SCHEMA;
  }

  @Override
  public boolean exists(String location) {
    Optional<S3Details> details = S3Details.from(location);
    if (!details.isPresent()) {
      return false;
    }

    return amazonS3.doesObjectExist(details.get().getBucket(), details.get().getKey());
  }

  @Override
  public List<String> files(String location, String... filter) {
    List<String> files = new ArrayList<>();

    Optional<S3Details> details = S3Details.from(location);
    if (!details.isPresent()) {
      return files;
    }

    ListObjectsRequest lor = new ListObjectsRequest()
        .withBucketName(details.get().getBucket())
        .withPrefix(StringUtils.appendIfMissing(details.get().getKey(), "/", "/"));

    ObjectListing listing = amazonS3.listObjects(lor);
    listing.getObjectSummaries().stream().forEach(os -> files.add(os.getKey()));

    while (listing.isTruncated()) {
      listing = amazonS3.listNextBatchOfObjects(listing);
      listing.getObjectSummaries().stream().forEach(os -> files.add(os.getKey()));
    }

    List<String> filters = Arrays.asList(filter);
    return files
        .stream()
        .filter(f -> checkFilter(f, filters))
        .map(f -> S3_SCHEMA + details.get().getBucket() + "/" + f)
        .collect(Collectors.toList());
  }

  private boolean checkFilter(String file, List<String> filters) {
    return filters.stream().anyMatch(f -> StringUtils.endsWith(file, f));
  }

  @Override
  public Optional<InputStream> open(String location) {
    if (log.isDebugEnabled()) {
      log.debug("Open S3 file: " + location);
    }
    Optional<S3Details> details = S3Details.from(location);

    if (details.isPresent()) {
      return Optional
          .of(amazonS3
              .getObject(new GetObjectRequest(details.get().getBucket(), details.get().getKey()))
              .getObjectContent());

    }

    return Optional.empty();
  }


  @Override
  public boolean move(File location, String outputLocation, boolean directory) {
    Optional<S3Details> details = S3Details.from(outputLocation);

    if (details.isPresent()) {

      MultipleFileUpload upload = transferManager
          .uploadDirectory(details.get().getBucket(), details.get().getKey(), location, true);

      if (log.isDebugEnabled()) {
        ProgressListener progressListener = progressEvent -> log
            .debug("S3 Transferred bytes: " + progressEvent.getBytesTransferred());
        upload.addProgressListener(progressListener);
      }

      try {
        upload.waitForCompletion();
      } catch (Exception e) {
        log.error("Error while uploading: {}");
        return false;
      }
    } else {
      log.error("Output location: {} is not vali, cannot upload data", outputLocation);
    }


    return true;

  }


  @Value
  public static class S3Details {
    private String bucket;
    private String key;

    public static Optional<S3Details> from(String location) {
      try {
        URI uri = new URI(location);
        return Optional
            .of(new S3Details(uri.getHost(), StringUtils.stripStart(uri.getPath(), "/")));
      } catch (URISyntaxException e) {
        log.error("Invalid S3 location: " + location);
      }
      return Optional.empty();
    }
  }

}

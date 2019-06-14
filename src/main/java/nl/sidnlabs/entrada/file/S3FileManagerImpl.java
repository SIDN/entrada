package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@Component("s3")
public class S3FileManagerImpl implements FileManager {

  private static final String S3_SCHEME = "s3://";
  private AmazonS3 amazonS3;

  @org.springframework.beans.factory.annotation.Value("${cloud.aws.upload.multipart.mb.size}")
  private int multipartSize;
  @org.springframework.beans.factory.annotation.Value("${cloud.aws.upload.parallelism}")
  private int parallelism;
  @org.springframework.beans.factory.annotation.Value("${cloud.aws.upload.upload.storage-class}")
  private String uploadStorageClass;
  @org.springframework.beans.factory.annotation.Value("${cloud.aws.upload.archive.storage-class}")
  private String archiveStorageClass;


  public S3FileManagerImpl(AmazonS3 amazonS3) {
    this.amazonS3 = amazonS3;
  }

  @Override
  public String schema() {
    return S3_SCHEME;
  }

  @Override
  public boolean exists(String location) {
    Optional<S3Details> details = S3Details.from(location);
    if (!details.isPresent()) {
      return false;
    }

    try {
      return amazonS3.doesObjectExist(details.get().getBucket(), details.get().getKey());
    } catch (AmazonServiceException e) {
      log.error("Error while checking if {} exists", location, e);
    }

    return false;
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
        .withPrefix(StringUtils.appendIfMissing(details.get().getKey(), "/", "/"))
        .withDelimiter("/");

    try {
      ObjectListing listing = amazonS3.listObjects(lor);
      listing.getObjectSummaries().stream().forEach(os -> files.add(os.getKey()));

      while (listing.isTruncated()) {
        listing = amazonS3.listNextBatchOfObjects(listing);
        listing.getObjectSummaries().stream().forEach(os -> files.add(os.getKey()));
      }
    } catch (Exception e) {
      log.error("Error while getting file listing for location {}", location, e);
      return Collections.emptyList();
    }

    List<String> filters = Arrays.asList(filter);
    return files
        .stream()
        .filter(f -> checkFilter(f, filters))
        .map(f -> S3_SCHEME + details.get().getBucket() + "/" + f)
        .collect(Collectors.toList());
  }

  private boolean checkFilter(String file, List<String> filters) {
    return filters.stream().anyMatch(f -> StringUtils.endsWith(file, f));
  }

  @Override
  public Optional<InputStream> open(String location) {
    log.debug("Open S3 file: " + location);
    Optional<S3Details> details = S3Details.from(location);

    if (details.isPresent()) {
      try {
        return Optional
            .of(amazonS3
                .getObject(new GetObjectRequest(details.get().getBucket(), details.get().getKey()))
                .getObjectContent());
      } catch (Exception e) {
        log.error("Cannot open {}", location, e);
      }

    }

    return Optional.empty();
  }


  @Override
  public boolean upload(String location, String outputLocation, boolean archive) {
    log.info("Upload work location: {} to target location: {}", location, outputLocation);

    File src = new File(location);


    if (!src.exists()) {
      log.error("Location {} does not exist, cannot continue with upload");
      return false;
    }

    Optional<S3Details> dstDetails = S3Details.from(outputLocation);

    if (dstDetails.isPresent()) {

      if (src.isDirectory()) {
        return uploadDirectory(src, dstDetails.get(), archive);
      } else {
        return uploadFile(src, dstDetails.get(), archive);
      }
    } else {
      log.error("Output location: {} is not valid, cannot upload data", outputLocation);
    }

    return true;

  }

  private boolean uploadFile(File src, S3Details dstDetails, boolean archive) {
    PutObjectRequest request = new PutObjectRequest(dstDetails.getBucket(),
        FileUtil.appendPath(dstDetails.getKey(), src.getName()), src);
    ObjectMetadata meta = new ObjectMetadata();

    if (archive) {
      meta
          .setHeader(Headers.STORAGE_CLASS,
              StorageClass.fromValue(StringUtils.upperCase(archiveStorageClass)));
    } else {
      meta
          .setHeader(Headers.STORAGE_CLASS,
              StorageClass.fromValue(StringUtils.upperCase(uploadStorageClass)));
    }

    request.setMetadata(meta);
    try {
      amazonS3.putObject(request);
      return true;
    } catch (Exception e) {
      log.error("Error while uploading: {}", src, e);
    }

    return false;
  }

  private boolean uploadDirectory(File location, S3Details dstDetails, boolean archive) {
    TransferManager transferManager = TransferManagerBuilder
        .standard()
        .withS3Client(amazonS3)
        .withMultipartUploadThreshold(multipartSize * 1024L * 1024L)
        .withExecutorFactory(() -> Executors.newFixedThreadPool(parallelism))
        .build();

    // make sure to set the storage class for newly uploaded files
    ObjectMetadataProvider metaDataProvider = (file, meta) -> {

      if (archive) {
        meta
            .setHeader(Headers.STORAGE_CLASS,
                StorageClass.fromValue(StringUtils.upperCase(archiveStorageClass)));
      } else {
        meta
            .setHeader(Headers.STORAGE_CLASS,
                StorageClass.fromValue(StringUtils.upperCase(uploadStorageClass)));
      }
    };


    MultipleFileUpload upload = transferManager
        .uploadDirectory(dstDetails.getBucket(), dstDetails.getKey(), location, true,
            metaDataProvider);


    if (log.isDebugEnabled()) {
      ProgressListener progressListener = progressEvent -> log
          .debug("S3 Transferred bytes: " + progressEvent.getBytesTransferred());
      upload.addProgressListener(progressListener);
    }

    try {
      upload.waitForCompletion();
      return true;
    } catch (Exception e) {
      log.error("Error while uploading: {}");
    } finally {
      transferManager.shutdownNow();
    }

    return false;
  }

  @Override
  public boolean move(String src, String dst) {
    log.info("Archive: {} to {}", src, dst);
    // s3 has no move operation, so do a copy and delete
    // this is not an atomic operation
    Optional<S3Details> srcDetails = S3Details.from(src);
    Optional<S3Details> dstDetails = S3Details.from(dst);

    if (srcDetails.isPresent() && dstDetails.isPresent()) {
      CopyObjectRequest cor = new CopyObjectRequest(srcDetails.get().getBucket(),
          srcDetails.get().getKey(), dstDetails.get().getBucket(), dstDetails.get().getKey());
      // make sure tos et the storage class for file copy
      cor.setStorageClass(StorageClass.fromValue(StringUtils.upperCase(archiveStorageClass)));
      try {
        amazonS3
            .copyObject(srcDetails.get().getBucket(), srcDetails.get().getKey(),
                dstDetails.get().getBucket(), dstDetails.get().getKey());
        return true;
      } catch (Exception e) {
        log.error("Error during copying {} to ", src, dst, e);
      }
    }
    return false;
  }

  @Override
  public boolean delete(String location) {
    log.info("Delete S3 file: " + location);

    Optional<S3Details> details = S3Details.from(location);

    if (details.isPresent()) {
      try {
        amazonS3.deleteObject(details.get().getBucket(), details.get().getKey());
        return true;
      } catch (Exception e) {
        log.error("Error while trying to delete {}", location, e);
      }
    }
    return false;
  }

  @Override
  public boolean supported(String location) {
    try {
      URI uri = new URI(location);
      return StringUtils.equalsIgnoreCase(uri.getScheme(), "s3");
    } catch (URISyntaxException e) {
      log.error("Invalid location URI: " + location);
    }
    return false;
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

  @Override
  public boolean isLocal() {
    return false;
  }

}

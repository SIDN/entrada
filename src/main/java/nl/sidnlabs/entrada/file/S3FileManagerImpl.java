package nl.sidnlabs.entrada.file;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.StorageClass;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@Component("s3")
public class S3FileManagerImpl implements FileManager {

  private static final String S3_SCHEME = "s3://";
  private AmazonS3 amazonS3;

  @Value("${aws.encryption}")
  protected boolean encrypt;

  @Value("${aws.upload.upload.storage-class}")
  private String uploadStorageClass;
  @Value("${aws.upload.archive.storage-class}")
  private String archiveStorageClass;

  // private TransferManager transferManager;

  public S3FileManagerImpl(AmazonS3 amazonS3, @Value("${aws.upload.parallelism}") int parallelism,
      @Value("${aws.upload.multipart.mb.size:5}") int multipartSize) {
    this.amazonS3 = amazonS3;

    // this.transferManager = TransferManagerBuilder
    // .standard()
    // .withS3Client(amazonS3)
    // .withMultipartUploadThreshold(multipartSize * 1024L * 1024L)
    // .withExecutorFactory(() -> Executors.newFixedThreadPool(parallelism))
    // .build();
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
  public List<String> files(String location, boolean recursive, String... filter) {
    List<String> files = new ArrayList<>();

    Optional<S3Details> details = S3Details.from(location);
    if (!details.isPresent()) {
      return files;
    }

    ListObjectsV2Request lor = new ListObjectsV2Request()
        .withBucketName(details.get().getBucket())
        .withPrefix(StringUtils.appendIfMissing(details.get().getKey(), "/", "/"));

    try {
      ListObjectsV2Result listing;

      do {
        listing = amazonS3.listObjectsV2(lor);
        listing.getObjectSummaries().stream().forEach(os -> files.add(os.getKey()));
        lor.setContinuationToken(listing.getNextContinuationToken());
      } while (listing.isTruncated());

    } catch (Exception e) {
      log.error("Error while getting file listing for location {}", location, e);
      return Collections.emptyList();
    }

    return files
        .stream()
        .filter(f -> checkFilter(f, Arrays.asList(filter)))
        .map(f -> S3_SCHEME + details.get().getBucket() + "/" + f)
        .collect(Collectors.toList());
  }

  private boolean checkFilter(String file, List<String> filters) {
    if (filters.isEmpty()) {
      return true;
    }
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



  /**
   * Upload new parquet files or archive processed pcap files. make sure to set the storage class
   * for newly uploaded files that are archived. the normal parquet upload must use s3 standard
   * storage class because there is no minimum day limit for standard class. after compaction the
   * files are changed to STANDARD-IA
   */
  @Override
  public boolean upload(String location, String outputLocation, boolean archive) {
    log.info("Upload work location: {} to target location: {}", location, outputLocation);

    File src = new File(location);
    if (!src.exists()) {
      log.error("Location {} does not exist, cannot continue with upload", location);
      return false;
    }

    Optional<S3Details> dstDetails = S3Details.from(outputLocation);

    if (dstDetails.isPresent()) {
      // network issues can cause upload to fail, do a simple 2nd retry if 1st try fails
      if (src.isFile()) {
        return uploadFile(src, dstDetails.get(), archive)
            || uploadFile(src, dstDetails.get(), archive);
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
    }

    if (encrypt) {
      meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    }

    request.setMetadata(meta);
    try {
      amazonS3.putObject(request);
      return true;
    } catch (Exception e) {
      log.error("Error while uploading file: {}", src, e);
    }

    return false;
  }


  // private boolean uploadDirectory(File location, S3Details dstDetails, boolean archive) {
  //
  // ObjectMetadataProvider metaDataProvider = (file, meta) -> {
  //
  // if (archive) {
  // meta
  // .setHeader(Headers.STORAGE_CLASS,
  // StorageClass.fromValue(StringUtils.upperCase(archiveStorageClass)));
  // }
  //
  // if (encrypt) {
  // meta.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
  // }
  // };
  //
  // MultipleFileUpload upload = transferManager
  // .uploadDirectory(dstDetails.getBucket(), dstDetails.getKey(), location, true,
  // metaDataProvider);
  //
  //
  // if (log.isDebugEnabled()) {
  // ProgressListener progressListener = progressEvent -> log
  // .debug("S3 Transferred bytes: " + progressEvent.getBytesTransferred());
  // upload.addProgressListener(progressListener);
  // }
  //
  // try {
  // upload.waitForCompletion();
  // return true;
  // } catch (Exception e) {
  // log.error("Error while uploading directory: {}", location, e);
  // }
  //
  // return false;
  // }

  @Override
  public boolean move(String src, String dst, boolean archive) {
    log.info("Move: {} to {}", src, dst);
    // s3 has no move operation, so do a copy and delete
    // this is not an atomic operation
    Optional<S3Details> srcDetails = S3Details.from(src);
    Optional<S3Details> dstDetails = S3Details.from(dst);

    if (srcDetails.isPresent() && dstDetails.isPresent()) {
      CopyObjectRequest cor = new CopyObjectRequest(srcDetails.get().getBucket(),
          srcDetails.get().getKey(), dstDetails.get().getBucket(), dstDetails.get().getKey());
      // make sure to set the storage class for file copy
      if (archive) {
        // set class for archive file
        cor.setStorageClass(StorageClass.fromValue(StringUtils.upperCase(archiveStorageClass)));
      } else {
        // set class for parquet files
        cor.setStorageClass(StorageClass.fromValue(StringUtils.upperCase(uploadStorageClass)));
      }
      try {
        CopyObjectResult r = amazonS3.copyObject(cor);

        if (Objects.nonNull(r.getETag())) {
          // copy ok, delete src
          amazonS3.deleteObject(srcDetails.get().getBucket(), srcDetails.get().getKey());
        }

        return true;
      } catch (Exception e) {
        log.error("Error during copying {} to ", src, dst, e);
      }
    }
    return false;
  }

  @Override
  public boolean rmdir(String location) {
    log.info("Delete S3 file: " + location);

    Optional<S3Details> details = S3Details.from(location);
    if (details.isPresent()) {

      // delete all objects with "location" prefix
      List<String> objects = files(location, false);
      objects.stream().forEach(o -> {
        Optional<S3Details> childDetails = S3Details.from(o);
        deleteObject(childDetails.get());
      });

      // everything ok
      return true;
    }
    return false;
  }

  @Override
  public boolean delete(String location) {
    log.info("Delete S3 file: " + location);

    Optional<S3Details> details = S3Details.from(location);
    if (details.isPresent()) {
      // delete file
      deleteObject(details.get());
      // everything ok
      return true;
    }
    return false;
  }

  private boolean deleteObject(S3Details object) {
    try {
      amazonS3.deleteObject(object.getBucket(), object.getKey());
      return true;
    } catch (Exception e) {
      log.error("Error while trying to delete {}", object, e);
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


  @lombok.Value
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

  @Override
  public boolean mkdir(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean chown(String path, String owner, String group) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> expired(String location, int maxAge, String... filter) {
    // return empty list, expiration handling is done using s3 policies
    return Collections.emptyList();
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public boolean chmod(String path, String permDir, String permFile) {
    // do nothing
    return true;
  }

}

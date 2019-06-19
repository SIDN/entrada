package nl.sidnlabs.entrada.file;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface FileManager {

  String schema();

  boolean exists(String location);

  boolean isLocal();

  /**
   * Is the location supported by the file manager
   * 
   * @param location location to check
   * @return true if the file manager supports the location for operations
   */
  boolean supported(String location);

  List<String> files(String location, String... filter);

  Optional<InputStream> open(String location);

  /**
   * Upload a local directory to another location, this can be local, S3 or HDFS
   * 
   * @param location location of the data to upload
   * @param outputLocation location where the data should be uploaded to
   * @param archive if true then the upload is to archive the data
   * @return true if the data has been uploaded successfully
   */
  boolean upload(String location, String outputLocation, boolean archive);

  boolean delete(String file, boolean children);

  boolean move(String src, String dest, boolean archive);

}

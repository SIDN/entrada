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
   * @param src location of the data to upload
   * @param dst location where the data should be uploaded to
   * @param archive if true then the upload is to archive the data
   * @return true if the data has been uploaded successfully
   */
  boolean upload(String src, String dst, boolean archive);

  /**
   * Delete a file
   * 
   * @param file the file to delete
   * @return true if file has been deleted or did not exist
   */
  boolean delete(String file);

  /**
   * Delete a directory and its children
   * 
   * @param dir the directory to delete
   * @return true if file has been deleted or did not exist
   */
  boolean rmdir(String dir);

  boolean move(String src, String dst, boolean archive);

  boolean mkdir(String path);

  boolean chown(String path, String owner, String group);

}

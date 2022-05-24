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

  List<String> files(String location, boolean recursive, String... filter);

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

  boolean chown(String path, String owner, String group, boolean recursive);

  public boolean chmod(String path, String permDir, String permFile, boolean recursive);

  /**
   * Get a list of files that have expired, meaning that are older than maxAge days. This will
   * recursively scan for files.
   * 
   * @param location the location to check (recusively)
   * @param maxAge number of days after which a file is expired
   * @return
   */
  List<String> expired(String location, int maxAge, String... filter);

  void close();

  /**
   * Chown without owner:group uses FS defaults
   * 
   * @param path
   */
  boolean chown(String path, boolean recursive);

  /**
   * Chmod without dir and file permissions uses FS defaults
   * 
   * @param path
   */
  boolean chmod(String path, boolean recursive);

}

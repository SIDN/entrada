package nl.sidnlabs.entrada.service;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@Component
public class UploadService {

  @Value("${entrada.location.work}")
  private String workLocation;

  @Value("${entrada.location.output}")
  private String outputLocation;

  @Value("${entrada.database.table.dns}")
  private String tableNameDns;

  @Value("${entrada.database.table.icmp}")
  private String tableNameIcmp;

  @Value("${entrada.icmp.enable}")
  private boolean icmpEnabled;

  private ServerContext serverCtx;
  private FileManagerFactory fileManagerFactory;
  private QueryEngine queryEngine;

  public UploadService(ServerContext serverCtx, FileManagerFactory fileManagerFactory,
      QueryEngine queryEngine) {
    this.serverCtx = serverCtx;
    this.fileManagerFactory = fileManagerFactory;
    this.queryEngine = queryEngine;
  }


  /**
   * Move created data from the work location to the output location. The original data will be
   * deleted.
   * 
   * @param partitions the partitions to upload
   * @param clean if true delete all the work location contents when done ( removes .crc files)
   */
  public void upload(Map<String, Set<Partition>> partitions, boolean clean) {
    FileManager fmOutput = fileManagerFactory.getFor(outputLocation);

    // move data to the database location on local or remote fs
    String location = locationForDNS();
    FileManager fmInput = fileManagerFactory.getFor(location);

    if (partitions.get("dns") != null && new File(location).exists()) {
      String dstLocation = FileUtil.appendPath(outputLocation, tableNameDns);

      for (Partition partition : partitions.get("dns")) {

        List<String> filesToUpload =
            fmInput.files(FileUtil.appendPath(location, partition.toPath()), true, ".parquet");

        for (String fileToUpload : filesToUpload) {

          if (fmOutput
              .upload(fileToUpload, FileUtil.appendPath(dstLocation, partition.toPath()), false)) {

            // delete uploaded files
            fmInput.delete(fileToUpload);
            /*
             * make sure the database table contains all the required partitions. If not create the
             * missing database partition(s)
             */
          }
        }
        queryEngine.addPartition("dns", tableNameDns, partition);

        // only delete work loc when upload was ok, if upload failed
        // it will be retried next time
        if (clean) {
          log.info("Delete work location: {}", location);
          fmInput.rmdir(location);
        }
      }
    }


    if (icmpEnabled && partitions.get("icmp") != null) {
      // move icmp data
      location = locationForICMP();

      if (new File(location).exists()) {
        String dstLocation = FileUtil.appendPath(outputLocation, tableNameIcmp);

        for (Partition partition : partitions.get("icmp")) {

          List<String> filesToUpload =
              fmInput.files(FileUtil.appendPath(location, partition.toPath()), true, ".parquet");

          for (String fileToUpload : filesToUpload) {

            if (fmOutput
                .upload(fileToUpload, FileUtil.appendPath(dstLocation, partition.toPath()),
                    false)) {

              // delete uploaded files
              fmInput.delete(fileToUpload);
              /*
               * make sure the database table contains all the required partitions. If not create
               * the missing database partition(s)
               */
            }
          }
          queryEngine.addPartition("dns", tableNameDns, partition);

          // only delete work loc when upload was ok, if upload failed
          // it will be retried next time
          if (clean) {
            log.info("Delete work location: {}", location);
            fmInput.rmdir(location);
          }
        }
      }
    }
  }


  private String locationForDNS() {
    return FileUtil.appendPath(workLocation, serverCtx.getServerInfo().getNormalizedName(), "dns/");
  }

  private String locationForICMP() {
    return FileUtil
        .appendPath(workLocation, serverCtx.getServerInfo().getNormalizedName(), "icmp/");
  }


}

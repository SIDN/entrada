package nl.sidnlabs.entrada.service;

import java.io.File;
import java.util.Date;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.model.jpa.FileArchive;
import nl.sidnlabs.entrada.model.jpa.FileArchiveRepository;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@Component
public class ArchiveService {

  public enum ArchiveOption {
    NONE, ARCHIVE, DELETE;
  }

  @Value("${entrada.location.archive}")
  private String archiveLocation;

  private ArchiveOption archiveOption;

  private FileManagerFactory fileManagerFactory;
  private FileArchiveRepository fileArchiveRepository;
  private ServerContext serverContext;

  public ArchiveService(FileManagerFactory fileManagerFactory,
      FileArchiveRepository fileArchiveRepository,
      @Value("${entrada.pcap.archive.mode}") String archiveMode, ServerContext serverContext) {

    this.fileManagerFactory = fileManagerFactory;
    this.fileArchiveRepository = fileArchiveRepository;
    this.archiveOption = ArchiveOption.valueOf(StringUtils.upperCase(archiveMode));
    this.serverContext = serverContext;
  }

  public boolean exists(String file, String server) {
    File f = new File(file);
    return fileArchiveRepository.findByFileAndServer(f.getName(), server) != null;
  }

  /**
   * Archive (move) data from src to target dst for archival, supported modes of operation are:
   * local2hdfs, local2s3, local2local, s32s3, hdfs2hsfs
   * 
   * @param file the file to archive
   * @param start time the file was processed
   * @param packets the number of packets found in the file
   */
  @Transactional
  public void archive(String file, Date start, int packets) {
    File f = new File(file);
    FileManager fmSrc = fileManagerFactory.getFor(file);

    if (ArchiveOption.ARCHIVE == archiveOption) {
      // move the pcap file to the archive location
      log.info("Archive: {} with mode: {}", file, archiveOption);

      FileManager fmDst = fileManagerFactory.getFor(archiveLocation);

      if (!fmSrc.isLocal() && !StringUtils.equals(fmDst.schema(), fmSrc.schema())) {
        // no support for moving data between different remote fs
        // e.g. hdfs -> s3
        log
            .error("Illegal archive operation, move from {} to {} not allowed", fmSrc.schema(),
                fmDst.schema());

        return;
      }

      if (StringUtils.equals(fmSrc.schema(), fmDst.schema())) {
        // move data on local fs OR on the same remote fs
        String dst = FileUtil
            .appendPath(archiveLocation, serverContext.getServerInfo().getName(), f.getName());

        if (!fmSrc.move(file, dst, true)) {
          log.error("File archiving failed for: {} to archive location: {} ", file, dst);
        }
      } else if (fmSrc.isLocal() && !fmDst.isLocal()) {
        // move data from local to remote fs
        fmDst
            .upload(file,
                FileUtil.appendPath(archiveLocation, serverContext.getServerInfo().getName()),
                true);
      }
    }

    // delete pcap file when archive or delete option is chosen
    if (ArchiveOption.ARCHIVE == archiveOption || ArchiveOption.DELETE == archiveOption) {
      fmSrc.delete(file);
    }

    // add the file to the database
    if (!exists(file, serverContext.getServerInfo().getName())) {
      Date now = new Date();
      FileArchive fa = FileArchive
          .builder()
          .dateEnd(now)
          .file(f.getName())
          .path(f.getParent())
          .server(serverContext.getServerInfo().getName())
          .dateStart(start)
          .rows(packets)
          .time((int) (now.getTime() - start.getTime()) / 1000)
          .build();

      fileArchiveRepository.save(fa);
    }
  }


}

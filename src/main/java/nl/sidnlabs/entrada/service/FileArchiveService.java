package nl.sidnlabs.entrada.service;

import java.io.File;
import java.util.Date;
import javax.transaction.Transactional;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.config.ServerContext;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.model.jpa.FileArchive;
import nl.sidnlabs.entrada.model.jpa.FileArchiveRepository;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@Component
public class FileArchiveService {

  public enum ArchiveOption {
    NONE, ARCHIVE, DELETE;
  }

  @Value("${entrada.location.archive}")
  private String archiveLocation;

  private ArchiveOption archiveOption;

  private FileManagerFactory fileManagerFactory;
  private FileArchiveRepository fileArchiveRepository;
  private ServerContext serverContext;

  public FileArchiveService(FileManagerFactory fileManagerFactory,
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
   * @param file
   * @param start
   * @param packets
   */
  @Transactional
  public void archive(String file, Date start, int packets) {
    log.info("Archive: {} with mode {}", file, archiveOption);
    // keep track of processed files

    Date now = new Date();
    File f = new File(file);
    FileArchive fa = FileArchive
        .builder()
        .dateEnd(now)
        .file(f.getName())
        .path(f.getParent())
        .server(f.getParentFile().getName())
        .dateStart(start)
        .rows(packets)
        .time((int) (now.getTime() - start.getTime()) / 1000)
        .build();

    fileArchiveRepository.save(fa);

    FileManager fmSrc = fileManagerFactory.getFor(file);
    // archive the pcap file
    if (ArchiveOption.ARCHIVE == archiveOption) {
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
            .appendPath(archiveLocation, serverContext.getServerInfo().getFullname(), f.getName());

        fmSrc.move(file, dst, true);
      } else if (fmSrc.isLocal() && !fmDst.isLocal()) {
        // move data from local to remote fs
        fmDst
            .upload(file,
                FileUtil.appendPath(archiveLocation, serverContext.getServerInfo().getFullname()),
                true);
      }
    }

    // delete pcap file when archive or delete option is chosen
    if (ArchiveOption.ARCHIVE == archiveOption || ArchiveOption.DELETE == archiveOption) {
      fmSrc.delete(file);
    }
  }


}

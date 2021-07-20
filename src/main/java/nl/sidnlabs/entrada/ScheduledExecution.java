package nl.sidnlabs.entrada;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.enrich.geoip.GeoIPService;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.load.PacketProcessor;

@Log4j2
@Component
public class ScheduledExecution {

  private ServerContext serverCtx;
  private ApplicationContext ctx;
  private SharedContext sharedContext;
  private List<FileManager> fileManagers;

  @Value("${entrada.nameservers}")
  private String servers;

  @Autowired
  private GeoIPService geoIPService;

  private Timer processTimer;

  public ScheduledExecution(ServerContext serverCtx, ApplicationContext ctx, MeterRegistry registry,
      SharedContext sharedContext, List<FileManager> fileManagers) {

    this.serverCtx = serverCtx;
    this.ctx = ctx;
    this.sharedContext = sharedContext;
    this.fileManagers = fileManagers;
    processTimer = registry.timer("processor.execution.time");
  }

  @Scheduled(fixedDelayString = "#{${entrada.execution.delay}*1000}")
  public void run() {

    if (!sharedContext.isEnabled()) {
      // processing not enabled
      log.info("Processing new PCAP data is currently not enabled");
      return;
    }

    sharedContext.setExecutionStatus(true);

    log.info("Checking if maxmind DB is up to date");
    // make sure the database are only downloaded once when using multiple geoip lookup instances
    geoIPService.initialize();
    log.info("Start loading data for: {}", servers);

    // create new processor each time, to avoid caches getting too big or having
    // memory leaks leading to OOM Exceptions

    if (StringUtils.isBlank(servers)) {
      // no individual servers configured, assume the pcap data is in the input location root dir
      runForServer("", ctx.getBean(PacketProcessor.class));
    } else {
      // individual servers configured, process each server directory
      Arrays
          .stream(StringUtils.split(servers, ","))
          .forEach(s -> runForServer(s, ctx.getBean(PacketProcessor.class)));
    }

    // cleanup filesystems, make sure all cached data and locked files are cleanup up
    fileManagers.stream().forEach(FileManager::close);

    sharedContext.setExecutionStatus(false);

    log.info("Completed loading name server data");
  }

  private void runForServer(String server, PacketProcessor processor) {
    log.info("Start loading data for: {}", server);

    if (!sharedContext.isEnabled()) {
      // processing not enabled
      log.info("Processing new PCAP data is currently not enabled");
      return;
    }

    serverCtx.setServer(server);

    try {
      // record time spent while processing all pcap files
      processTimer.record(processor::execute);
    } catch (Exception e) {
      log.error("Error while processing pcap data for: {}", server, e);
    }

    log.info("Completed loading data for: {}", server);
  }

}

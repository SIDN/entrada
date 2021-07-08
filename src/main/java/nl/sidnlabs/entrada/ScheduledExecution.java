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
import nl.sidnlabs.entrada.enrich.resolver.DnsResolverCheck;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.load.PacketProcessor;

@Log4j2
@Component
public class ScheduledExecution {

  private ServerContext serverCtx;
  private ApplicationContext applicationContext;
  private SharedContext sharedContext;
  private List<DnsResolverCheck> resolverChecks;
  private List<FileManager> fileManagers;

  @Value("${entrada.nameservers}")
  private String servers;

  @Autowired
  private GeoIPService geoIPService;

  private Timer processTimer;

  public ScheduledExecution(ServerContext serverCtx, ApplicationContext applicationContext,
      MeterRegistry registry, SharedContext sharedContext, List<FileManager> fileManagers,
      List<DnsResolverCheck> resolverChecks) {

    this.serverCtx = serverCtx;
    this.applicationContext = applicationContext;
    this.resolverChecks = resolverChecks;
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
    geoIPService.initialize();
    log.info("Start loading data for name servers: {}", servers);

    // initialize DnsResolverCheck to make sure they use uptodate data
    // resolverChecks.stream().forEach(DnsResolverCheck::init);

    // create new processor each time, to avoid caches getting too big or having
    // memory leaks leading to OOM Exceptions
    PacketProcessor processor = applicationContext.getBean(PacketProcessor.class);

    if (StringUtils.isBlank(servers)) {
      // no individual servers configured, assume the pcap data is in the input location root dir
      runForServer("", processor);
    } else {
      // individual servers configured, process each server directory
      Arrays.stream(StringUtils.split(servers, ",")).forEach(s -> runForServer(s, processor));
    }

    // cleanup filesystems, make sure all cached data and locked files are cleanup up
    fileManagers.stream().forEach(FileManager::close);

    sharedContext.setExecutionStatus(false);

    // resolverChecks.stream().forEach(DnsResolverCheck::done);

    log.info("Completed loading name server data");
  }

  private void runForServer(String server, PacketProcessor processor) {
    log.info("Start loading name data for server: {}", server);

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
      log.error("Error while processing pcap data for name server: {}", server, e);
    }

    log.info("Completed loading data for name server: {}", server);
  }

}

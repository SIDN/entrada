package nl.sidnlabs.entrada;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.config.ServerContext;
import nl.sidnlabs.entrada.enrich.resolver.DnsResolverCheck;
import nl.sidnlabs.entrada.load.PacketProcessor;
import nl.sidnlabs.entrada.metric.MetricManager;

@Log4j2
@Component
public class ScheduledExecution {

  private ServerContext settings;
  private MetricManager metricManager;
  private PacketProcessor pcapProcessor;
  private List<DnsResolverCheck> resolverChecks;

  @Value("${entrada.serverlist}")
  private String servers;

  public ScheduledExecution(ServerContext settings, MetricManager metricManager,
      PacketProcessor pcapProcessor, List<DnsResolverCheck> resolverChecks) {

    this.settings = settings;
    this.metricManager = metricManager;
    this.pcapProcessor = pcapProcessor;
    this.resolverChecks = resolverChecks;
  }


  @Scheduled(fixedDelayString = "#{${entrada.execution.delay}*1000}")
  public void run() {
    // show resolver info to make sure the resolver data has been loaded
    if (log.isDebugEnabled()) {
      resolverChecks.stream().forEach(r -> {
        r.init();
        log.debug("Loaded {} IP subnets for {} resolver service", r.getSize(), r.getName());
      });
    }

    if (StringUtils.isBlank(servers)) {
      // no individual servers configured, assume the pcap data is in the input location root dir
      runForServer("");
    } else {
      // individual servers configured, process each server directory
      Arrays.stream(StringUtils.split(servers, ",")).forEach(this::runForServer);
    }
  }

  private void runForServer(String server) {
    log.info("Start loading data for server: {}", server);
    long start = System.currentTimeMillis();

    settings.setServer(server);

    try {
      pcapProcessor.execute();
    } catch (Exception e) {
      log.error("Error while loading data:", e);
      metricManager.send(MetricManager.METRIC_IMPORT_RUN_ERROR_COUNT, 1);
    } finally {
      // always send stats to monitoring
      long end = System.currentTimeMillis();
      int runtimeSecs = (int) (end - start) / 1000;
      metricManager.send(MetricManager.METRIC_IMPORT_RUN_TIME, runtimeSecs);
      metricManager.send(MetricManager.METRIC_IMPORT_RUN_ERROR_COUNT, 0);
      metricManager.flush();
    }

    log.info("Done loading data for server: {}", server);
  }

}

package nl.sidnlabs.entrada;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.enrich.resolver.DnsResolverCheck;
import nl.sidnlabs.entrada.load.PacketProcessor;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;

@Log4j2
@Component
public class ScheduledExecution {

  private ServerContext serverCtx;
  private PacketProcessor pcapProcessor;
  private List<DnsResolverCheck> resolverChecks;
  private HistoricalMetricManager metricManager;

  @Value("${entrada.nameservers}")
  private String servers;

  private Counter okCounter;
  private Counter failCounter;
  private Timer processTimer;

  public ScheduledExecution(ServerContext serverCtx, PacketProcessor pcapProcessor,
      List<DnsResolverCheck> resolverChecks, MeterRegistry registry,
      HistoricalMetricManager metricManager) {

    this.serverCtx = serverCtx;
    this.pcapProcessor = pcapProcessor;
    this.resolverChecks = resolverChecks;
    this.metricManager = metricManager;

    okCounter = registry.counter("processor.ok");
    failCounter = registry.counter("processor.fail");
    processTimer = registry.timer("processor.time");
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

    serverCtx.setServer(server);

    try {
      // record time spent while processing all pcap files
      processTimer.record(() -> pcapProcessor.execute());
      okCounter.increment();
    } catch (Exception e) {
      log.error("Error while processing pcap data for server: {}", server, e);
      failCounter.increment();
    } finally {
      // always send historical stats to monitoring
      if (!metricManager.flush()) {
        log.error("Not all metrics have been sent to Graphite");
      }
    }

    log.info("Done loading data for server: {}", server);
  }

}

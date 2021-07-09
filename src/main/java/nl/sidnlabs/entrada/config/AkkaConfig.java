package nl.sidnlabs.entrada.config;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class AkkaConfig {

  @Value("${entrada.writer.dns.count:1}")
  private int dnsWriters;
  @Value("${entrada.writer.icmp.count:1}")
  private int icmpWriters;

  @Value("${entrada.row.builder.count:1}")
  private int rowbuilderCount;


  @Bean
  public Config config() {

    Map<String, Object> cfgMap = new HashMap<>();
    cfgMap.putAll(createThreadConfig("reader-dispatcher", 1, "PinnedDispatcher"));
    cfgMap.putAll(createThreadConfig("metrics-dispatcher", 1, "PinnedDispatcher"));
    cfgMap.putAll(createThreadConfig("decoder-dispatcher", 1, "PinnedDispatcher"));
    cfgMap
        .putAll(
            createThreadConfig("writer-dispatcher", dnsWriters + icmpWriters, "PinnedDispatcher"));
    cfgMap.putAll(createThreadConfig("row-builder-dispatcher", rowbuilderCount, "Dispatcher"));

    return ConfigFactory.load().withFallback(ConfigFactory.parseMap(cfgMap));

  }

  private Map<String, Object> createThreadConfig(String name, int poolSize, String dspType) {
    log.info("Create akka dispatcher {} size = {}", name, poolSize);

    Map<String, Object> cfg = new HashMap<>();
    cfg.put(name + ".type", dspType);
    cfg.put(name + ".executor", "thread-pool-executor");
    cfg.put(name + ".thread-pool-executor.fixed-pool-size", String.valueOf(poolSize));
    cfg.put(name + ".throughput", "1");
    return cfg;
  }

}

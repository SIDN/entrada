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
  @Value("${entrada.row.decoder.count:2}")
  private int rowDecoderCount;
  @Value("${entrada.row.builder.count:1}")
  private int rowbuilderCount;


  @Bean
  public Config config() {

    Map<String, Object> cfgMap = new HashMap<>();
    cfgMap.putAll(createForJoinPoolConfig("entrada-dispatcher"));
    // cfgMap
    // .putAll(createThreadConfig("entrada-writer-dispatcher", dnsWriters + icmpWriters, 1,
    // "PinnedDispatcher"));

    return ConfigFactory.load().withFallback(ConfigFactory.parseMap(cfgMap));
  }

  private Map<String, Object> createForJoinPoolConfig(String name) {
    log.info("Create akka fork-join-executor: {}", name);

    Map<String, Object> cfg = new HashMap<>();
    cfg.put(name + ".type", "Dispatcher");
    cfg.put(name + ".executor", "fork-join-executor");
    return cfg;
  }

  // private Map<String, Object> createThreadConfig(String name, int size, int tp, String dp) {
  // log.info("Create akka thread-pool-executor {}", name);
  //
  // Map<String, Object> cfg = new HashMap<>();
  // cfg.put(name + ".type", dp);
  // cfg.put(name + ".executor", "thread-pool-executor");
  // cfg.put(name + ".thread-pool-executor.fixed-pool-size", "" + size);
  // cfg.put(name + ".throughput", "" + tp);
  // return cfg;
  // }

}

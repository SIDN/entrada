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

  @Value("${entrada.stream.thread.count:3}")
  private int threads;

  @Value("${entrada.stream.thread.througput:10}")
  private int throughput;

  private int defaultDispatcherThreads = 4;

  @Bean
  public Config config() {

    Map<String, Object> cfgMap = new HashMap<>();
    cfgMap.putAll(createForJoinPoolConfig("entrada-dispatcher"));

    // make sure the dynamic thread config created above is leadign and may override default akka
    // cfg.
    return ConfigFactory.load(ConfigFactory.parseMap(cfgMap).withFallback(ConfigFactory.load()));
  }

  private Map<String, Object> createForJoinPoolConfig(String name) {
    log.info("Create akka fork-join-executor: {}", name);
    Map<String, Object> cfg = new HashMap<>();

    // create thread config for the dispatcher that is goiugn to do the heavy lifting.
    cfg.put(name + ".type", "Dispatcher");
    cfg.put(name + ".executor", "fork-join-executor");
    cfg.put(name + ".fork-join-executor.parallelism-min", "" + threads);
    cfg.put(name + ".fork-join-executor.parallelism-max", "" + threads);
    cfg.put(name + ".throughput", "" + throughput);

    // only use limited set of threads for default-dispatcher
    cfg.put("akka.actor.default-dispatcher.fork-join-executor.parallelism-min", "1");
    cfg
        .put("akka.actor.default-dispatcher.fork-join-executor.parallelism-max",
            "" + defaultDispatcherThreads);
    return cfg;
  }

}

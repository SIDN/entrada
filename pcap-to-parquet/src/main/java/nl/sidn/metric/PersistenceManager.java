package nl.sidn.metric;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class PersistenceManager {

  private MetricManager metricManager;
  @Value("${entrada.cache.timeout}")
  private int timeout;

  public PersistenceManager(MetricManager metricManager) {
    this.metricManager = metricManager;
  }


  public void persist(Kryo kryo, Output output) {
    // only keep newly created, updated and non-expired metrics
    Map<String, Metric> metricCache = metricManager.getMetricCache();
    // get the max ttl for cache items, add 10 minutes just to be safe
    // in cases where transporting the file to entrada takes longer
    int cacheTimeoutInSecs = (timeout + 10) * 60;
    long now = System.currentTimeMillis();

    Map<String, Metric> newMap = new HashMap<>();
    int expired = 0;
    for (String key : metricCache.keySet()) {
      Metric m = metricCache.get(key);

      long keyMaxTTLInMillis = (m.getTime() + cacheTimeoutInSecs) * 1000;

      // check if key was updated this round, if so then also keep it.
      if (m != null && m.isAlive()) {
        newMap.put(key, m);
      } else if (keyMaxTTLInMillis > now) {
        // key has not yet expired, keep around and put in put to persist
        newMap.put(key, m);
      } else {
        expired++;
      }
    }
    log.info("Found " + expired + " expired cache entries");
    // contains only the "live" metrics
    kryo.writeObject(output, newMap);

    log.info("Persist statistics cache with " + newMap.size() + " metrics");
  }

  public void load(Kryo kryo, Input input) {
    Map<String, Metric> metricCache = kryo.readObject(input, HashMap.class);

    // mark metric as non-alive, so if not updated this run, then it will not be persisted again
    metricCache.entrySet().stream().forEach(e -> e.getValue().setAlive(false));

    log.info("Loaded statistics cache " + metricCache.size() + " metrics");
    metricManager.setMetricCache(metricCache);
  }

}

/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidnlabs.entrada.metric;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.load.StateManager;
import nl.sidnlabs.entrada.model.Row;


/**
 * MetricManager is used to recreate metrics for DNS packets found in PCAP files. The timestamp of a
 * packets in the PCAP file is used when generating the metrics and NOT the timestamp at the point
 * in time when the packet was read from the PCAP.
 */
@Log4j2
@Component
@Data
public class HistoricalMetricManager {

  // do not send last minute
  private static final int FLUSH_TIMESTAMP_WAIT = 10;

  // dns stats
  public static final String METRIC_IMPORT_DNS_QUERY_COUNT = "dns.query";
  public static final String METRIC_IMPORT_DNS_RESPONSE_COUNT = "dns.response";

  public static final String METRIC_IMPORT_DNS_QTYPE = "dns.request.qtype";
  public static final String METRIC_IMPORT_DNS_RCODE = "dns.request.rcode";
  public static final String METRIC_IMPORT_DNS_OPCODE = "dns.request.opcode";

  // layer 4 stats
  public static final String METRIC_IMPORT_TCP_COUNT = "tcp";
  public static final String METRIC_IMPORT_UDP_COUNT = "udp";
  public static final String METRIC_IMPORT_ICMP_COUNT = "icmp";

  public static final String METRIC_IMPORT_IP_VERSION_4_COUNT = "ip.4";
  public static final String METRIC_IMPORT_IP_VERSION_6_COUNT = "ip.6";

  public static final String METRIC_IMPORT_COUNTRY_COUNT = "geo.country";

  public static final String METRIC_IMPORT_TCP_HANDSHAKE_RTT = "tcp.rtt.handshake.median";
  public static final String METRIC_IMPORT_TCP_HANDSHAKE_RTT_SAMPLES = "tcp.rtt.handshake.samples";
  public static final String METRIC_IMPORT_TCP_PACKET_RTT = "tcp.rtt.packet.median";
  public static final String METRIC_IMPORT_TCP_PACKET_RTT_SAMPLES = "tcp.rtt.packet.samples";

  @Value("${management.metrics.export.graphite.enabled:true}")
  protected boolean metricsEnabled;

  private int metricListCounter = 0;
  private int metricCounter = 0;

  private Map<String, TreeMap<Long, Metric>> metricCache = new ConcurrentHashMap<>(1000);

  @Value("${management.metrics.export.graphite.prefix}")
  private String prefix;

  @Value("${management.metrics.export.graphite.host}")
  private String host;

  @Value("${management.metrics.export.graphite.port}")
  private int port;

  @Value("${management.metrics.export.graphite.retention}")
  private int retention = 10;

  private ServerContext settings;

  public HistoricalMetricManager(ServerContext settings) {
    this.settings = settings;
  }

  private String createMetricName(String metric) {
    // replace dot in the server name with underscore otherwise graphite will assume nesting

    return new StringBuilder()
        .append(prefix)
        .append(".")
        .append(metric)
        .append(".ns.")
        .append(StringUtils.defaultIfBlank(settings.getServerInfo().getNormalizedName(), "all"))
        .toString();
  }

  public void update(Row row) {
    if (!metricsEnabled || row.getMetrics() == null) {
      // do nothing
      return;
    }

    for (Metric m : row.getMetrics()) {
      if (m != null) {
        update(m);
      }
    }
  }


  private void update(Metric m) {
    long metricTime = round(m.getTime());
    Long time = Long.valueOf(metricTime);
    String metricName = createMetricName(m.getName());
    TreeMap<Long, Metric> metricValues = metricCache.get(metricName);

    if (metricValues == null) {
      // create new treemap
      metricValues = new TreeMap<>();
      metricValues.put(time, m);
      metricCache.put(metricName, metricValues);
    } else {

      Metric mHist = metricValues.get(time);
      if (mHist != null) {
        mHist.update(m.getValue());
      } else {
        metricValues.put(time, m);
      }
    }
  }

  public static Metric createMetric(String metric, int value, long timestamp, boolean simple) {
    if (simple) {
      return new SimpleMetric(metric, value, timestamp);
    }
    return new MeanMetric(metric, value, timestamp);
  }

  /**
   * Uses a threshhold to determine if the value should be sent to graphite low values may indicate
   * trailing queries in later pcap files. duplicate timestamps get overwritten by graphite and only
   * the last timestamp value is used by graphite.
   */
  public boolean flush() {
    log.info("Flushing metrics to Graphite, size: {}", metricCache.size());

    int oldSize = metricCache.size();

    metricCache
        .entrySet()
        .stream()
        .forEach(e -> log.info("Metric: {}  entries: {}", e.getKey(), e.getValue().size()));

    GraphiteSender graphite = new Graphite(host, port);
    try {
      graphite.connect();
      // send each metrics to graphite
      metricCache.entrySet().stream().map(Entry::getValue).forEach(m -> send(graphite, m));
    } catch (Exception e) {
      // cannot connect connect to graphite
      log.error("Could not connect to Graphite", e);
      return false;
    } finally {
      // remove sent metric, avoiding sending them again.
      metricCache.values().stream().forEach(this::trunc);
      // check if any metric has an empty list of time-buckets, if so the list
      metricCache.entrySet().removeIf(e -> e.getValue().size() == 0);

      try {
        // close will also do a flush
        graphite.close();
      } catch (Exception e) {
        // ignore
      }
    }

    int newSize = metricCache.size();

    log.info("-------------- Metrics Manager Stats ---------------------");
    log.info("Metrics processed: {}", metricCounter);
    log.info("Metrics count before flush: {}", oldSize);
    log.info("Metrics count after flush: {}", newSize);


    return true;
  }

  public void clear() {
    metricCache.clear();
  }

  private void trunc(TreeMap<Long, Metric> metricValues) {
    int max = metricValues.size() - FLUSH_TIMESTAMP_WAIT;
    if (max < 1) {
      // no metrics to send
      return;
    }
    List<Long> toDelete = metricValues.keySet().stream().limit(max).collect(Collectors.toList());
    toDelete.stream().forEach(metricValues::remove);
  }

  private void send(GraphiteSender graphite, TreeMap<Long, Metric> metricValues) {
    // do not send the last FLUSH_TIMESTAMP_WAIT timestamps to prevent duplicate timestamps
    // being sent to graphite. (only the last will count) and will cause dips in charts
    int max = metricValues.size() - FLUSH_TIMESTAMP_WAIT;
    if (max < 1) {
      // no metrics to send
      return;
    }
    metricValues.entrySet().stream().limit(max).forEach(e -> send(graphite, e.getValue()));
  }

  private void send(GraphiteSender graphite, Metric m) {
    try {
      graphite.send(m.getName(), String.valueOf(m.getValue()), m.getTime());
      if (m.getSamples() > 0) {
        graphite
            .send(StringUtils.replace(m.getName(), ".median", ".samples"),
                String.valueOf(m.getSamples()), m.getTime());
      }
    } catch (IOException e) {
      log.error("Error while sending metric: {}", m, e);
    }
  }


  private long round(long millis) {
    // get retention from config
    long secs = (millis / 1000);
    return secs - (secs % retention);
  }

  public void persistState(StateManager stateManager) {
    stateManager.write(metricCache);
  }

  public int size() {
    return metricCache.size();
  }

  public void loadState(StateManager stateManager) {
    metricCache = (Map<String, TreeMap<Long, Metric>>) stateManager.read();
    if (metricCache == null) {
      metricCache = new HashMap<>();
    }
  }

}

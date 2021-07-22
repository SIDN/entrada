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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.load.StateManager;
import nl.sidnlabs.entrada.model.BaseMetricValues;
import nl.sidnlabs.entrada.model.DnsMetricValues;
import nl.sidnlabs.entrada.model.IcmpMetricValues;


/**
 * MetricManager is used to recreate metrics for DNS packets found in PCAP files. The timestamp of a
 * packets in the PCAP file is used when generating the metrics and NOT the timestamp at the point
 * in time when the packet was read from the PCAP.
 */
@Log4j2
@Component
@Getter
@Setter
public class HistoricalMetricManager {

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

  public static final String METRIC_IMPORT_TCP_HANDSHAKE_RTT = "tcp.rtt.handshake.avg";
  public static final String METRIC_IMPORT_TCP_HANDSHAKE_RTT_SAMPLES = "tcp.rtt.handshake.samples";


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

  public void update(BaseMetricValues mv) {

    if (!metricsEnabled || mv == null) {
      // do nothing
      return;
    }

    Long time = Long.valueOf(roundToRetention(mv.time));

    if (mv instanceof IcmpMetricValues) {
      update(METRIC_IMPORT_ICMP_COUNT, time, 1, true);

      return;
    }

    DnsMetricValues dmv = (DnsMetricValues) mv;
    if (dmv.dnsQuery) {
      update(METRIC_IMPORT_DNS_QUERY_COUNT, time, 1, true);
    }

    if (dmv.dnsResponse) {
      update(METRIC_IMPORT_DNS_RESPONSE_COUNT, time, 1, true);
    }

    update(METRIC_IMPORT_DNS_QTYPE + "." + dmv.dnsQtype, time, 1, true);
    update(METRIC_IMPORT_DNS_RCODE + "." + dmv.dnsRcode, time, 1, true);
    update(METRIC_IMPORT_DNS_OPCODE + "." + dmv.dnsOpcode, time, 1, true);
    update(METRIC_IMPORT_COUNTRY_COUNT + "." + dmv.country, time, 1, true);

    if (dmv.ProtocolUdp) {
      update(METRIC_IMPORT_UDP_COUNT, time, 1, true);
    } else {
      update(METRIC_IMPORT_TCP_COUNT, time, 1, true);
      if (dmv.tcpHandshake != -1) {
        update(METRIC_IMPORT_TCP_HANDSHAKE_RTT, time, dmv.tcpHandshake, false);
      }
    }

    if (dmv.ipV4) {
      update(METRIC_IMPORT_IP_VERSION_4_COUNT, time, 1, true);
    } else {
      update(METRIC_IMPORT_IP_VERSION_6_COUNT, time, 1, true);
    }
  }

  private void update(String name, Long time, int value, boolean counter) {

    TreeMap<Long, Metric> metricValues = metricCache.get(name);

    if (metricValues == null) {
      // create new treemap
      metricValues = new TreeMap<>();
      Metric m = createMetric(name, value, time.longValue(), counter);
      metricValues.put(time, m);
      metricCache.put(m.getName(), metricValues);
    } else {

      Metric mHist = metricValues.get(time);
      if (mHist != null) {
        mHist.update(value);
      } else {
        Metric m = createMetric(name, value, time.longValue(), counter);
        metricValues.put(time, m);
      }
    }
  }

  public static Metric createMetric(String metric, int value, long timestamp, boolean counter) {
    if (counter) {
      return new SumMetric(metric, value, timestamp);
    }
    return new AvgMetric(metric, value, timestamp);
  }

  private long roundToRetention(long millis) {
    // get retention from config
    long secs = (millis / 1000);
    return secs - (secs % retention);
  }

  /**
   * Uses a threshhold to determine if the value should be sent to graphite low values may indicate
   * trailing queries in later pcap files. duplicate timestamps get overwritten by graphite and only
   * the last timestamp value is used by graphite.
   */
  public void flush() {
    log.info("Flushing metrics to Graphite, size: {}", metricCache.size());

    if (!metricsEnabled) {
      // do nothing
      return;
    }

    int oldSize = metricCache.size();

    metricCache
        .entrySet()
        .stream()
        .forEach(e -> log.info("Metric: {}  datapoints: {}", e.getKey(), e.getValue().size()));

    GraphiteSender graphite = new Graphite(host, port);
    try {
      graphite.connect();
      // send each metrics to graphite
      metricCache.entrySet().stream().map(Entry::getValue).forEach(m -> send(graphite, m));
    } catch (Exception e) {
      // cannot connect connect to graphite
      log.error("Could not connect to Graphite", e);
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
  }

  public void clear() {
    metricCache = new ConcurrentHashMap<>(1000);
  }

  private void trunc(TreeMap<Long, Metric> metricValues) {
    int limit = metricValues.size() - 1;
    if (limit == 0) {
      // no metrics to send
      return;
    }
    List<Long> toDelete = metricValues.keySet().stream().limit(limit).collect(Collectors.toList());
    toDelete.stream().forEach(metricValues::remove);
  }

  private void send(GraphiteSender graphite, TreeMap<Long, Metric> metricValues) {
    // do not send the last timestamp to prevent duplicate timestamp
    // being sent to graphite. (only the last will count) and will cause dips in charts
    int limit = metricValues.size() - 1;
    if (limit == 0) {
      // no metrics to send
      return;
    }
    metricValues.entrySet().stream().limit(limit).forEach(e -> send(graphite, e.getValue()));
  }

  private void send(GraphiteSender graphite, Metric m) {
    if (m.isCached() && !m.isUpdated()) {
      // old cached metric from previous run, was not updated this run
      // most likely due to wrong order in pcaps.
      // ignore this metric, otherwise we get dips in the charts
      return;
    }

    String fqMetricName = createMetricName(m.getName());
    try {
      graphite.send(fqMetricName, String.valueOf(m.getValue()), m.getTime());
      if (m instanceof AvgMetric) {
        graphite
            .send(StringUtils.replace(fqMetricName, ".avg", ".samples"),
                String.valueOf(m.getSamples()), m.getTime());
      }
    } catch (IOException e) {
      log.error("Error while sending metric: {}", m, e);
    }
  }


  public void persistState(StateManager stateManager) {
    for (Map.Entry<String, TreeMap<Long, Metric>> e : metricCache.entrySet()) {
      for (Map.Entry<Long, Metric> metric : e.getValue().entrySet()) {
        metric.getValue().setCached();
      }

    }
    stateManager.writeObject(metricCache);
  }

  public int size() {
    return metricCache.size();
  }

  public void loadState(StateManager stateManager) {
    metricCache = (Map<String, TreeMap<Long, Metric>>) stateManager.readObject();
    if (metricCache == null) {
      metricCache = new HashMap<>();
    }
  }

}

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
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteSender;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;


/**
 * MetricManager is used to recreate metrics for DNS packets found in PCAP files. The timestamp of a
 * packets in the PCAP file is used when generating the metrics and NOT the timestamp at the point
 * in time when the packet was read from the PCAP.
 */
@Log4j2
@Component
public class HistoricalMetricManager {

  // dns stats
  public static final String METRIC_IMPORT_DNS_QUERY_COUNT = "hist.packetDns.query.count";
  public static final String METRIC_IMPORT_DNS_RESPONSE_COUNT = "hist.packetDns.response.count";

  public static final String METRIC_IMPORT_DNS_QTYPE = "hist.packetDns.request.qtype";
  public static final String METRIC_IMPORT_DNS_RCODE = "hist.packetDns.request.rcode";
  public static final String METRIC_IMPORT_DNS_OPCODE = "hist.packetDns.request.opcode";

  // layer 4 stats
  public static final String METRIC_IMPORT_TCP_COUNT = "hist.tcp.packet.count";
  public static final String METRIC_IMPORT_UDP_COUNT = "hist.udp.packet.count";

  public static final String METRIC_IMPORT_IP_VERSION_4_COUNT = "hist.ip.version.4.count";
  public static final String METRIC_IMPORT_IP_VERSION_6_COUNT = "hist.ip.version.6.count";

  private Map<String, Metric> metricCache = new HashMap<>();

  @Value("${management.metrics.export.graphite.prefix}")
  private String prefix;

  @Value("${management.metrics.export.graphite.host}")
  private String host;

  @Value("${management.metrics.export.graphite.port}")
  private int port;

  @Value("${management.metrics.export.graphite.retention}")
  private int retention;


  private ServerContext settings;

  public HistoricalMetricManager(ServerContext settings) {
    this.settings = settings;
  }

  private String createMetricName(String metric) {
    // replace dot in the server name with underscore otherwise graphite will assume nesting
    return prefix + "." + metric + ".ns." + servername();
  }

  private String servername() {
    if (StringUtils.isBlank(settings.getServerInfo().getNormalizeName())) {
      // no server used, then use general purpose "all"
      return "all";
    }

    return settings.getServerInfo().getNormalizeName();
  }


  public void send(String metric, int value, long timestamp) {
    long metricTime = round(timestamp);
    String metricName = createMetricName(metric);
    String lookup = metricName + "." + metricTime;
    Metric m = metricCache.get(lookup);
    if (m != null) {
      m.update(value);
    } else {
      metricCache.put(lookup, new Metric(metricName, value, metricTime));
    }
  }

  public boolean flush() {
    log.info("Flushing metrics to Graphite");
    if (log.isDebugEnabled()) {
      metricCache
          .entrySet()
          .stream()
          .forEach(e -> log.debug("Metric: {}  value: {}", e.getKey(), e.getValue()));
    }

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
      metricCache.clear();
      try {
        // close will also do a flush
        graphite.close();
      } catch (Exception e) {
        // ignore
      }
    }

    return true;
  }

  private void send(GraphiteSender graphite, Metric m) {
    try {
      graphite.send(m.getName(), String.valueOf(m.getValue()), m.getTime());
    } catch (IOException e) {
      log.error("Error while sending metric: {}", m, e);
    }
  }

  private long round(long millis) {
    // return ((millis + 500) / 1000);
    // get retention from config
    long secs = (millis / 1000);
    return secs - (secs % retention);
  }


  protected Map<String, Metric> getMetricCache() {
    return metricCache;
  }

  protected void setMetricCache(Map<String, Metric> metricCache) {
    this.metricCache = metricCache;
  }

  @Data
  public class Metric implements Comparable<Metric> {

    private String name;
    private long value;
    private long time;

    public Metric(String name, long value, long time) {
      this.name = name;
      this.value = value;
      this.time = time;
    }

    public void update(long value) {
      this.value += value;
    }

    @Override
    public String toString() {
      return name + " " + value + " " + time;
    }

    @Override
    public int compareTo(Metric o) {
      return Integer.compare((int) this.time, (int) o.getTime());
    }

  }
}

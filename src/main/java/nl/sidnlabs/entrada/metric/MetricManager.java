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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.config.ServerContext;


/**
 * MetricManager is used to recreate metrics for DNS packets found in a PCAP file. The timestamp of
 * a packets in the PCAP file is used when generating the metrics and NOT the timestamp of the point
 * in time when the packet was read from the PCAP.
 *
 */
@Log4j2
@Component
public class MetricManager {

  // dns stats
  public static final String METRIC_IMPORT_DNS_QUERY_COUNT = ".dns.request.count";
  public static final String METRIC_IMPORT_DNS_RESPONSE_COUNT = ".dns.response.count";
  public static final String METRIC_IMPORT_DNS_NO_REQUEST_COUNT = ".dns.response.norequest.count";
  public static final String METRIC_IMPORT_DNS_QTYPE = ".dns.request.qtype";
  public static final String METRIC_IMPORT_DNS_RCODE = ".dns.request.rcode";
  public static final String METRIC_IMPORT_DNS_OPCODE = ".dns.request.opcode";
  public static final String METRIC_IMPORT_DNS_NO_RESPONSE_COUNT = ".dns.noreresponse.count";

  // layer 4 stats
  public static final String METRIC_IMPORT_DNS_TCPSTREAM_COUNT = ".dns.tcp.session.count";
  public static final String METRIC_IMPORT_TCP_COUNT = ".tcp.packet.count";
  public static final String METRIC_IMPORT_UDP_COUNT = ".udp.packet.count";

  public static final String METRIC_IMPORT_UDP_REQUEST_FRAGMENTED_COUNT =
      ".udp.request.fragmented.count";
  public static final String METRIC_IMPORT_UDP_RESPONSE_FRAGMENTED_COUNT =
      ".udp.response.fragmented.count";
  public static final String METRIC_IMPORT_TCP_REQUEST_FRAGMENTED_COUNT =
      ".tcp.request.fragmented.count";
  public static final String METRIC_IMPORT_TCP_RESPONSE_FRAGMENTED_COUNT =
      ".tcp.response.fragmented.count";
  public static final String METRIC_IMPORT_IP_VERSION_4_COUNT = ".ip.version.4.count";
  public static final String METRIC_IMPORT_IP_VERSION_6_COUNT = ".ip.version.6.count";

  public static final String METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE = ".dns.response.bytes.size";
  public static final String METRIC_IMPORT_DNS_QUERY_BYTES_SIZE = ".dns.request.bytes.size";

  // decoder app stats
  public static final String METRIC_IMPORT_DNS_COUNT = ".dns.message.count";
  public static final String METRIC_IMPORT_FILES_COUNT = ".files.count";
  public static final String METRIC_IMPORT_RUN_TIME = ".time.duration";
  public static final String METRIC_IMPORT_RUN_ERROR_COUNT = ".run.error.count";

  public static final String METRIC_IMPORT_TCP_PREFIX_ERROR_COUNT = ".tcp.prefix.error.count";

  public static final String METRIC_IMPORT_STATE_PERSIST_UDP_FLOW_COUNT =
      ".state.persist.udp.flow.count";
  public static final String METRIC_IMPORT_STATE_PERSIST_TCP_FLOW_COUNT =
      ".state.persist.tcp.flow.count";
  public static final String METRIC_IMPORT_STATE_PERSIST_DNS_COUNT = ".state.persist.dns.count";

  // icmp
  public static final String METRIC_ICMP_COUNT = ".icmp.packet.count";
  public static final String METRIC_ICMP_V4 = ".icmp.v4";
  public static final String METRIC_ICMP_V6 = ".icmp.v6";
  public static final String METRIC_ICMP_PREFIX_TYPE_V4 = ".icmp.v4.prefix.type";
  public static final String METRIC_ICMP_PREFIX_TYPE_V6 = ".icmp.v6.prefix.type";
  public static final String METRIC_ICMP_ERROR = ".icmp.error";
  public static final String METRIC_ICMP_INFO = ".icmp.info";

  // cache stats
  public static final String METRIC_IMPORT_CACHE_EXPPIRED_DNS_QUERY_COUNT =
      ".cache.expired.dns.request.count";

  private Map<String, Metric> metricCache = new HashMap<>();
  private List<Metric> realtimeMetrics = new ArrayList<>();

  @Value("${graphite.prefix}")
  private String graphitePrefix;

  @Value("${graphite.retention}")
  private int retention;
  @Value("${graphite.threshhold}")
  private int threshhold;

  private ServerContext settings;
  private GraphiteAdapter graphiteAdapter;

  public MetricManager(ServerContext settings, GraphiteAdapter graphiteAdapter) {
    this.settings = settings;
    this.graphiteAdapter = graphiteAdapter;
  }

  /**
   * send overall metrics, use current system time
   * 
   * @param metric name of the metric
   * @param value value of the metric
   */
  public void send(String metric, int value) {
    String metricName = createMetricName(metric);
    TimeZone timeZone = TimeZone.getTimeZone("UTC");
    Calendar calendar = Calendar.getInstance(timeZone);
    realtimeMetrics.add(new Metric(metricName, value, calendar.getTimeInMillis() / 1000));
  }

  private String createMetricName(String metric) {
    // replace dot in the server name with underscore otherwise graphite will assume nesting
    return graphitePrefix + "." + servername() + metric;
  }

  private String servername() {
    if (StringUtils.isBlank(settings.getServerInfo().getFullname())) {
      // no server used, then use general purpose "all"
      return "all";
    }

    return StringUtils
        .trimToEmpty(StringUtils.replace(settings.getServerInfo().getFullname(), ".", "_"));
  }


  public void sendAggregated(String metric, int value, long timestamp, boolean useThreshHold) {
    long metricTime = roundTimestamp(timestamp);
    String metricName = createMetricName(metric);
    String lookup = metricName + "." + metricTime;
    Metric m = metricCache.get(lookup);
    if (m != null) {
      m.update(value);
    } else {
      metricCache.put(lookup, new Metric(metricName, value, metricTime, useThreshHold));
    }
  }

  /**
   * send aggregated counts (per server) aggregate by 10s bucket
   * 
   * @param metric name of the metric
   * @param value value of the metric
   * @param timestamp timestamp of metric
   */
  public void sendAggregated(String metric, int value, long timestamp) {
    sendAggregated(metric, value, timestamp, true);
  }

  public void flush() {
    if (log.isDebugEnabled()) {
      log.debug("Write metrics to queue");
      metricCache
          .entrySet()
          .stream()
          .forEach(e -> log.debug("Metric {}  value: ", e.getKey(), e.getValue()));
    }

    graphiteAdapter.connect();
    if (graphiteAdapter.isConnected()) {
      StringBuilder buffer = new StringBuilder();
      metricCache.entrySet().stream().forEach(e -> appendMetric(buffer, e.getValue()));

      realtimeMetrics
          .stream()
          .filter(Objects::nonNull)
          .forEach(e -> buffer.append(e.toString() + "\n"));


      String data = buffer.toString();
      log.debug("Sending metrics");
      if (log.isDebugEnabled()) {
        log.debug("Metric: {}", data);
      }
      graphiteAdapter.send(data);
      graphiteAdapter.close();
    }
  }

  private void appendMetric(StringBuilder buffer, Metric m) {
    /**
     * Use a threshhold to determine if the value should be sent to graphite low values may indicate
     * trailing queries in later pcap files. duplicate timestamps get overwritten by graphite and
     * only the last timestamp value is used by graphite.
     */
    if (m.isUseThreshHold() && m.getValue() < threshhold) {
      log
          .debug("Metric " + m.getName() + " is below threshold min of " + threshhold
              + " with actual value of " + m.getValue());
    } else {
      // add metric to graphite plaintext protocol string
      // @see:
      // http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol
      buffer.append(m.toString() + "\n");
    }
  }


  /**
   * Round the timestamp to the nearest retention time
   * 
   * @param intime
   * @return
   */
  private long roundTimestamp(long intime) {
    // get retention from config
    long offset = (intime % retention);
    return intime - offset;
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
    private boolean useThreshHold;
    private boolean alive;

    public Metric(String name, long value, long time) {
      this(name, value, time, true);
    }

    public Metric(String name, long value, long time, boolean useThreshHold) {
      this.name = name;
      this.value = value;
      this.time = time;
      this.useThreshHold = useThreshHold;
      this.alive = true;
    }

    public void update(long value) {
      this.value += value;
      this.alive = true;
    }

    /**
     * @return pickle format, which can be sent to graphite.
     */
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

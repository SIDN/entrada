package nl.sidnlabs.entrada.model;

public class DnsMetricValues extends BaseMetricValues {

  public boolean dnsQuery;
  public boolean dnsResponse;
  public int dnsQtype;
  public int dnsRcode;
  public int dnsOpcode;

  public boolean ProtocolUdp;

  public boolean ipV4;
  public String country;

  public int tcpHandshake = -1;

  public DnsMetricValues(long time) {
    super(time);
  }



  // public enum MetricName {
  // METRIC_IMPORT_DNS_QUERY_COUNT("dns.query"),
  // METRIC_IMPORT_DNS_RESPONSE_COUNT("dns.response"),
  // METRIC_IMPORT_DNS_QTYPE("dns.request.qtype"),
  // METRIC_IMPORT_DNS_RCODE("dns.request.rcode"),
  // METRIC_IMPORT_DNS_OPCODE("dns.request.opcode"),
  // // layer 4 stats
  // METRIC_IMPORT_TCP_COUNT("tcp"),
  // METRIC_IMPORT_UDP_COUNT("udp"),
  // METRIC_IMPORT_ICMP_COUNT("icmp"),
  //
  // METRIC_IMPORT_IP_VERSION_4_COUNT("ip.4"),
  // METRIC_IMPORT_IP_VERSION_6_COUNT("ip.6"),
  //
  // METRIC_IMPORT_COUNTRY_COUNT("geo.country"),
  //
  // METRIC_IMPORT_TCP_HANDSHAKE_RTT("tcp.rtt.handshake.median"),
  // METRIC_IMPORT_TCP_HANDSHAKE_RTT_SAMPLES("tcp.rtt.handshake.samples"),
  // METRIC_IMPORT_TCP_PACKET_RTT("tcp.rtt.packet.median"),
  // METRIC_IMPORT_TCP_PACKET_RTT_SAMPLES("tcp.rtt.packet.samples");
  //
  //
  // private MetricName(String value) {
  // this.value = value;
  // }
  //
  // private String value;
  //
  // public String getValue() {
  // return value;
  // }

  // }

}

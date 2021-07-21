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

}

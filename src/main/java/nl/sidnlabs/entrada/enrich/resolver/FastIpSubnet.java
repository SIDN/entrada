package nl.sidnlabs.entrada.enrich.resolver;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.handler.ipfilter.IpSubnetFilterRule;
import lombok.Getter;

@Getter
public class FastIpSubnet implements Comparable<FastIpSubnet> {

  private final IpSubnetFilterRule cidr;
  private String cidrString;

  public FastIpSubnet() {
    cidr = null;
  }

  /**
   * Create IpSubnet using the CIDR Notation<BR>
   * i.e.:<br>
   * IpSubnet subnet = new IpSubnet("10.10.10.0/24"); or<br>
   * IpSubnet subnet = new IpSubnet("1fff:0:0a88:85a3:0:0:0:0/24");
   *
   * @param netAddress a network address as string.
   */
  public FastIpSubnet(String netAddress) throws UnknownHostException {
    String[] split = netAddress.split("/");
    String cidr = split[0];
    int cidrPrefix = Integer.parseInt(split[1]);
    this.cidr = new IpSubnetFilterRule(cidr, cidrPrefix, IpFilterRuleType.ACCEPT);
    cidrString = cidr.toString();
  }

  /**
   * Compares the given InetAddress against the Subnet and returns true if the ip is in the
   * subnet-ip-range and false if not.
   *
   * @return returns true if the given IP address is inside the currently set network.
   */
  public boolean contains(InetAddress inetAddress) {
    if (cidr == null) {
      return false;
    }
    return cidr.matches(new InetSocketAddress(inetAddress, 0));
  }

  @Override
  public String toString() {
    return cidrString;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof FastIpSubnet)) {
      return false;
    }
    FastIpSubnet ipSubnet = (FastIpSubnet) o;
    return ipSubnet.cidr.equals(cidr);
  }

  @Override
  public int hashCode() {
    return cidr.hashCode();
  }

  /** Compare two IpSubnet */
  public int compareTo(FastIpSubnet o) {
    return cidr.compareTo(o.getCidr());
  }
}

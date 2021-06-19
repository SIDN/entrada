package nl.sidnlabs.entrada.enrich.resolver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.jboss.netty.handler.ipfilter.CIDR;
import org.jboss.netty.handler.ipfilter.IpSet;
import lombok.Getter;

@Getter
public class FastIpSubnet implements IpSet, Comparable<FastIpSubnet> {

  /** Internal representation */
  private final CIDR cidr;
  private String cidrString;

  /** Create IpSubnet for ALL (used for ALLOW or DENY ALL) */
  public FastIpSubnet() {
    // ALLOW or DENY ALL
    cidr = null;
  }

  /**
   * Create IpSubnet using the CIDR or normal Notation<BR>
   * i.e.:<br>
   * IpSubnet subnet = new IpSubnet("10.10.10.0/24"); or<br>
   * IpSubnet subnet = new IpSubnet("10.10.10.0/255.255.255.0"); or<br>
   * IpSubnet subnet = new IpSubnet("1fff:0:0a88:85a3:0:0:0:0/24");
   *
   * @param netAddress a network address as string.
   */
  public FastIpSubnet(String netAddress) throws UnknownHostException {
    cidr = CIDR.newCIDR(netAddress);
    cidrString = cidr.toString();
  }

  /**
   * Compares the given IP-Address against the Subnet and returns true if the ip is in the
   * subnet-ip-range and false if not.
   *
   * @param ipAddr an ipaddress
   * @return returns true if the given IP address is inside the currently set network.
   */
  // public boolean contains(InetAddress ipAddr) throws UnknownHostException {
  // //InetAddress inetAddress1 = InetAddress.getByName(ipAddr);
  // return contains(ipAddr);
  // }

  /**
   * Compares the given InetAddress against the Subnet and returns true if the ip is in the
   * subnet-ip-range and false if not.
   *
   * @return returns true if the given IP address is inside the currently set network.
   */
  public boolean contains(InetAddress inetAddress) {
    if (cidr == null) {
      // ANY
      return true;
    }
    return cidr.contains(inetAddress);
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
    return cidrString.compareTo(o.getCidrString());
  }
}

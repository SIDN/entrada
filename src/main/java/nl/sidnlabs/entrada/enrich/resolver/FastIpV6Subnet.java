package nl.sidnlabs.entrada.enrich.resolver;

import java.net.UnknownHostException;
import org.jboss.netty.handler.ipfilter.CIDR;
import com.googlecode.ipv6.IPv6Address;
import com.googlecode.ipv6.IPv6AddressRange;
import lombok.Getter;

@Getter
public class FastIpV6Subnet {

  private final IPv6AddressRange range;

  public FastIpV6Subnet() {
    range = null;
  }

  public FastIpV6Subnet(String netAddress) throws UnknownHostException {
    CIDR cidr = CIDR.newCIDR(netAddress);

    range = IPv6AddressRange
        .fromFirstAndLast(IPv6Address.fromInetAddress(cidr.getBaseAddress()),
            IPv6Address.fromInetAddress(cidr.getEndAddress()));

  }

  /**
   * Compares the given InetAddress against the Subnet and returns true if the ip is in the
   * subnet-ip-range and false if not.
   *
   * @return returns true if the given IP address is inside the currently set network.
   */
  public boolean contains(IPv6Address address) {
    if (range == null) {
      return false;
    }
    return range.contains(address);
  }

  @Override
  public String toString() {
    return range.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof FastIpV6Subnet)) {
      return false;
    }
    FastIpV6Subnet ipSubnet = (FastIpV6Subnet) o;
    return ipSubnet.range.equals(range);
  }

  @Override
  public int hashCode() {
    return range.hashCode();
  }

}

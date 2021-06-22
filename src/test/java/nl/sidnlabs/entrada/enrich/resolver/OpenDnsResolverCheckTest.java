package nl.sidnlabs.entrada.enrich.resolver;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import org.apache.commons.net.util.SubnetUtils;
import org.junit.Test;

public class OpenDnsResolverCheckTest {

  @Test
  public void testLoadResolverEndpointsOk() {

    OpenDNSResolverCheck check = new OpenDNSResolverCheck();
    check.setUrl("https://umbrella.cisco.com/why-umbrella/global-network-and-traffic");
    check.setTimeout(5);

    List<String> ips = check.fetch();
    assertTrue("No IPs found", ips.size() > 0);
  }


  @Test
  public void subnetTest() {
    SubnetUtils utils = new SubnetUtils("74.125.114.192/26");
    assertTrue(utils.getInfo().getAllAddresses().length > 0);
  }


  @Test
  public void ip2LongTest() {
    OpenDNSResolverCheck check = new OpenDNSResolverCheck();
    long value = check.ipToLong("173.194.169.16");
    assertEquals(2915215632L, value);
  }



}

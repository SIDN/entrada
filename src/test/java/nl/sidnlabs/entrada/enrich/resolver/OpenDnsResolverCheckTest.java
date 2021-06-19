package nl.sidnlabs.entrada.enrich.resolver;

import static org.junit.Assert.assertTrue;
import java.util.List;
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


}

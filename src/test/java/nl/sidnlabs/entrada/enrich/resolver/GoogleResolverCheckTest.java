package nl.sidnlabs.entrada.enrich.resolver;

import static org.junit.Assert.assertTrue;
import java.net.InetAddress;
import org.junit.Test;
import com.google.common.net.InetAddresses;

public class GoogleResolverCheckTest {

  @Test
  public void testMatchOk() {

    GoogleResolverCheck check = new GoogleResolverCheck();
    check.setHostname("locations.publicdns.goog");
    check.setWorkDir("/tmp/");
    check.setMaxMatchCacheSize(1000);
    check.init();

    InetAddress addr = InetAddresses.forString("173.194.169.16");
    assertTrue("No resolver match found", check.match(addr.getHostAddress(), addr));
    // try 2nd time see if cache is working correct
    assertTrue("No resolver match found", check.match(addr.getHostAddress(), addr));
  }

}

package nl.sidnlabs.entrada.enrich.resolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.junit.jupiter.api.Test;
import nl.sidnlabs.entrada.util.IpUtil;
import nl.sidnlabs.entrada.util.StringUtil;

public class OpenDnsResolverCheckTest {

  @Test
  public void testLoadResolverEndpointsOk() {

    OpenDNSResolverCheck check = new OpenDNSResolverCheck();
    check.setUrl("https://umbrella.cisco.com/why-umbrella/global-network-and-traffic");
    check.setTimeout(5);

    List<String> ips = check.fetch();
    assertTrue(ips.size() > 0, "No IPs found");
  }


  @Test
  public void subnetTest() {
    SubnetUtils utils = new SubnetUtils("74.125.114.192/26");
    assertTrue(utils.getInfo().getAllAddresses().length > 0);
  }


  @Test
  public void ip2LongTest() {
    long value = IpUtil.ipToLong("173.194.169.16");
    assertEquals(2915215632L, value);
  }

  @Test
  public final void ip2LongTestPerf() {

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      IpUtil.ipToLong("173.194.169.16");
    }
    System.out.println("IndexOf: " + (System.currentTimeMillis() - start) + "ms");


  }

  @Test
  public final void testFastSplitIpv4() {
    String[] r = StringUtil.splitFastIpv4("123.125.111.172");
    assertEquals("123", r[0]);
    assertEquals("125", r[1]);
    assertEquals("111", r[2]);
    assertEquals("172", r[3]);
  }

  @Test
  public final void testIndexOfPerf() {
    String line = "123.125.111.172";

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      StringUtil.splitFastIpv4(line);
    }
    System.out.println("IndexOf: " + (System.currentTimeMillis() - start) + "ms");

    start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      StringUtils.split(line, ".");
    }

    System.out.println("StringUtils: " + (System.currentTimeMillis() - start) + "ms");

  }



}

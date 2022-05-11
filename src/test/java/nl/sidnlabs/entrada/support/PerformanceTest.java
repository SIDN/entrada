package nl.sidnlabs.entrada.support;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.net.InetSocketAddress;

import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.handler.ipfilter.IpSubnetFilterRule;
import org.junit.jupiter.api.Test;
import com.googlecode.ipv6.IPv6Address;
import com.googlecode.ipv6.IPv6AddressRange;
import nl.sidnlabs.dnslib.util.DomainName;
import nl.sidnlabs.dnslib.util.InternetDomainName;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.dnslib.util.RegisteredDomain;

public class PerformanceTest {

  @Test
  public void testDomain() throws Exception {

    String tst = "www.sidn.nl";

    RegisteredDomain d1 = DomainName.registeredDomain(tst);
    InternetDomainName d2 = InternetDomainName.from(tst);

    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000000; i++) {
      String n = NameUtil.domainname(tst);
      int labels = NameUtil.labels(tst);
    }
    long time1 = System.currentTimeMillis() - start;
    System.out.println(time1);

    start = System.currentTimeMillis();
    for (int i = 0; i < 1000000; i++) {
      InternetDomainName d = InternetDomainName.from(tst);
    }
    long time2 = System.currentTimeMillis() - start;
    System.out.println(time2);

    assertTrue(time1 < time2);
  }


  @Test
  public void testIpv6() throws Exception {
    IPv6AddressRange range = IPv6AddressRange
        .fromFirstAndLast(IPv6Address.fromString("2a00:1450:4013::"),
            IPv6Address.fromString("2a00:1450:4013:ffff:ffff:ffff:ffff:ffff"));

    InetSocketAddress addr = new InetSocketAddress("2a00:1450:4013:0:0:0:0:8844", 0);

    IPv6Address addr1 = IPv6Address.fromString("2a00:1450:4013:0:0:0:0:8844");

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      range.contains(addr1);
    }
    long time1 = System.currentTimeMillis() - start;
    System.out.println(time1);

    IpSubnetFilterRule cidr1 = new IpSubnetFilterRule("2a00:1450:4013::", 48, IpFilterRuleType.ACCEPT);
    // IpSubnetFilterRule cidr2 = new IpSubnetFilterRule("fe80::226:2dff:fefa:ffff", 128, IpFilterRuleType.ACCEPT);


    start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      cidr1.matches(addr);
    }
    long time2 = System.currentTimeMillis() - start;
    System.out.println(time2);

    assertTrue(time1 < time2);
  }



}

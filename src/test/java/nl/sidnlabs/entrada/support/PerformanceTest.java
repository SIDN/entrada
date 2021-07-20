package nl.sidnlabs.entrada.support;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.jboss.netty.handler.ipfilter.CIDR;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import com.google.common.net.InetAddresses;
import com.googlecode.ipv6.IPv6Address;
import com.googlecode.ipv6.IPv6AddressRange;
import nl.sidnlabs.dnslib.util.DomainName;
import nl.sidnlabs.dnslib.util.InternetDomainName;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.dnslib.util.RegisteredDomain;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.model.DnsMetricValues;

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

    InetAddress addr = InetAddresses.forString("2a00:1450:4013:0:0:0:0:8844");

    IPv6Address addr1 = IPv6Address.fromString("2a00:1450:4013:0:0:0:0:8844");

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      range.contains(addr1);
    }
    long time1 = System.currentTimeMillis() - start;
    System.out.println(time1);

    CIDR cidr1 = CIDR.newCIDR("2a00:1450:4013::/48");
    // CIDR cidr2 = CIDR.newCIDR("fe80::226:2dff:fefa:ffff");


    start = System.currentTimeMillis();
    for (int i = 0; i < 100000; i++) {
      cidr1.contains(addr);
    }
    long time2 = System.currentTimeMillis() - start;
    System.out.println(time2);

    assertTrue(time1 < time2);
  }


  @Test
  public void testAutoboxing() throws Exception {

    long start = System.currentTimeMillis();
    Integer counter1 = 0;
    for (int i = 0; i < 1000000; i++) {
      counter1 = counter1 + (int) i;
    }
    long time1 = System.currentTimeMillis() - start;
    System.out.println(time1);

    start = System.currentTimeMillis();
    int counter2 = 0;
    for (int i = 0; i < 1000000; i++) {
      counter2 = counter2 + i;
    }
    long time2 = System.currentTimeMillis() - start;
    System.out.println(time2);


    start = System.currentTimeMillis();
    Integer counter3 = 0;
    for (int i = 0; i < 1000000; i++) {
      counter3 = Integer.valueOf((int) i);
    }
    long time3 = System.currentTimeMillis() - start;
    System.out.println(time3);

    Integer counter4 = 0;
    for (int i = 0; i < 1000000; i++) {
      counter3 = i;
    }
    long time4 = System.currentTimeMillis() - start;
    System.out.println(time4);


    assertTrue(time2 < time1);
  }

  public class FieldMapping {
    public int time = 0;
  }

  @Test
  public void testAvroRecordg() throws Exception {
    Schema schema = schema("/avro/dns-query.avsc");
    GenericRecord record = new GenericData.Record(schema);

    FieldMapping fm = new FieldMapping();


    long start = System.currentTimeMillis();
    for (int i = 0; i < 20000000; i++) {
      for (int i2 = 0; i2 < 20; i2++) {
        record.put("time", Integer.valueOf(0));
      }
    }
    long time1 = System.currentTimeMillis() - start;
    System.out.println(time1);

    start = System.currentTimeMillis();
    for (int i = 0; i < 20000000; i++) {
      for (int i2 = 0; i2 < 20; i2++) {
        record.put(fm.time, Integer.valueOf(0));
      }
    }
    long time2 = System.currentTimeMillis() - start;
    System.out.println(time2);
  }

  public Schema schema(String schema) {
    try {
      Parser parser = new Schema.Parser().setValidate(true);
      return parser.parse(new ClassPathResource(schema, getClass()).getInputStream());
    } catch (IOException e) {
      throw new ApplicationException("Cannot load schema from file: " + schema, e);
    }
  }

  @Test
  public void testMetrics() throws Exception {
    HistoricalMetricManager mm = new HistoricalMetricManager(null);
    mm.setMetricsEnabled(true);

    long start = System.currentTimeMillis();
    DnsMetricValues dmv = new DnsMetricValues(new Date().getTime());
    dmv.dnsQuery = true;
    dmv.dnsResponse = true;
    for (int i = 0; i < 1000000; i++) {
      if (i % 10000 == 0) {
        dmv.time = dmv.time + 5;
      }
      mm.update(dmv);
    }
    long time1 = System.currentTimeMillis() - start;
    System.out.println(time1);
  }


}

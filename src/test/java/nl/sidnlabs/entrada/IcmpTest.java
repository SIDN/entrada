package nl.sidnlabs.entrada;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.ICMPPacket;
import nl.sidnlabs.pcap.packet.Packet;

public class IcmpTest extends AbstractTest {

  @Test
  public void testIcmpWithPartialDnsPayloadOk() {
    // this pcap contains a single icmp packet response has a partial dns message payload
    PcapReader reader =
        createReaderFor("pcap/sidnlabs-test-icmp-dest-unreachable-1-packet-dns-payload.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(1, pckts.size());

    Packet p = pckts.get(0);
    assertNotEquals(Packet.NULL, p);

    assertTrue(p instanceof ICMPPacket);

    ICMPPacket icmp = (ICMPPacket) p;

    assertTrue(icmp.getOriginalIPPacket() instanceof DNSPacket);
    DNSPacket dns = (DNSPacket) icmp.getOriginalIPPacket();

    assertEquals(1, dns.getMessageCount());
    assertEquals("google.nl.", dns.getMessage().getQuestions().get(0).getQName());
    assertEquals(ResourceRecordType.DS, dns.getMessage().getQuestions().get(0).getQType());
  }


  @Test
  public void testIcmpWithIcmpPayloadOk() {
    // this pcap contains a single icmp packet where the payload is also an (partial) icmp packet
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-icmpv6-with-icmpv6-payload.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(1, pckts.size());

    Packet p = pckts.get(0);
    assertNotEquals(Packet.NULL, p);

    assertTrue(p instanceof ICMPPacket);
    ICMPPacket icmp = (ICMPPacket) p;
    assertEquals(3, icmp.getType());
    assertEquals(0, icmp.getCode());

    assertTrue(icmp.getOriginalIPPacket() instanceof ICMPPacket);
    ICMPPacket nestedIcmp = (ICMPPacket) icmp.getOriginalIPPacket();
    assertEquals(129, nestedIcmp.getType());
    assertEquals(0, nestedIcmp.getCode());
  }

  @Test
  public void testIcmpFragmentationNeededOk() {
    // this pcap contains a single icmp packet where the payload is also an (partial) icmp packet
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-icmp-frag-needed.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(1, pckts.size());

    Packet p = pckts.get(0);
    assertNotEquals(Packet.NULL, p);

    assertTrue(p instanceof ICMPPacket);
    ICMPPacket icmp = (ICMPPacket) p;
    assertEquals(3, icmp.getType());
    assertEquals(4, icmp.getCode());
    assertEquals(1500, icmp.getMtu());

  }



}

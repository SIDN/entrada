package nl.sidnlabs.entrada;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
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
    assertNotEquals(Packet.NULL, icmp);

    assertTrue(icmp.getOriginalIPPacket() instanceof DNSPacket);
    DNSPacket dns = (DNSPacket) icmp.getOriginalIPPacket();

    assertEquals(1, dns.getMessageCount());
    assertEquals("google.nl.", dns.getMessage().getQuestions().get(0).getQName());
    assertEquals(ResourceRecordType.DS, dns.getMessage().getQuestions().get(0).getQType());
  }

}

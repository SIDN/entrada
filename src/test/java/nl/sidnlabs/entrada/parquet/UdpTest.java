package nl.sidnlabs.entrada.parquet;

import static org.junit.Assert.assertEquals;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Packet;

public class UdpTest extends AbstractTest {


  @Test
  public void testUDP2DnsMessagesOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-udp-2-dns-ok.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    long count = pckts.stream().flatMap(p -> ((DNSPacket) p).getMessages().stream()).count();
    assertEquals(2, count);
  }


}

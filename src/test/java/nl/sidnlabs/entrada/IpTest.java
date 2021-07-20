package nl.sidnlabs.entrada;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.Packet;

public class IpTest extends AbstractTest {

  @Test
  public void testIpv4FragmentsOk() {
    // this pcap contains a single dns response that uses 2 ip fragments
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-ip-1-dns-response-2-ip-fragments.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(1, pckts.size());
    assertTrue(pckts.get(0).isFragmented());
    assertEquals(1480, pckts.get(0).getFragOffset());
    assertEquals(2, pckts.get(0).getReassembledFragments());
  }

}

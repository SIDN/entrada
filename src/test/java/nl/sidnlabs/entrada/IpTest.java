package nl.sidnlabs.entrada;

import static org.junit.Assert.assertEquals;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.Packet;

public class IpTest extends AbstractTest {

  @Test
  public void testIpFragmentsOk() {
    // this pcap contains a single dns response that uses 2 ip fragments
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-ip-1-dns-response-2-ip-fragments.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(1, pckts.size());
  }

}

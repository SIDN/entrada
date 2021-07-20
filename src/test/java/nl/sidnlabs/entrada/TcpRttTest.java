package nl.sidnlabs.entrada;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.Packet;

public class TcpRttTest extends AbstractTest {


  @Test
  public void testTCPHandshakeRttOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-tcp-handshake.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    long count = pckts.stream().filter(p -> p.getTcpHandshakeRTT() > 0).count();
    assertEquals(1, count);
  }

  @Test
  public void testTCPHandshakeRttSynRetransmissionOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-tcp-syn-retransmission.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    // retransmission of client SYN packets, the decoder must ignore the tcp handshake
    // because it is unclear to which packet the server responded.
    long count = pckts.stream().filter(p -> p.getTcpHandshakeRTT() > 0).count();
    assertEquals(0, count);
  }



  @Test
  public void testTCPPacketRttOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-tcp-handshake.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    // 1st packet only has handshake
    assertTrue(pckts.get(0).getTcpHandshakeRTT() > 0);

    assertFalse(pckts.get(1).getTcpHandshakeRTT() > 0);
  }

  @Test
  public void testMultipleTCPPacketRttOk() {
    PcapReader reader =
        createReaderFor("pcap/sidnlabs-test-multiple-dns-in-single-tcp-stream.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(4, pckts.size());

    // 1st packet only has handshake
    assertTrue(pckts.get(0).getTcpHandshakeRTT() > 0);
  }

  @Test
  public void testTCPStreamOk() {
    PcapReader reader =
        createReaderForComppressed("pcap/sidnlabs-test-tcp-stream-many-dns-msg-per-tcp-packet.gz");

    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(55, pckts.size());
  }


}

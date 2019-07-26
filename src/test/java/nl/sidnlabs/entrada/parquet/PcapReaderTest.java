package nl.sidnlabs.entrada.parquet;

import static org.junit.Assert.assertEquals;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Packet;

@Log4j2
public class PcapReaderTest {

  private PcapReader createReaderFor(String file) {
    ClassPathResource resource = new ClassPathResource(file);
    log.info("Load pcap from {}", resource);

    try (InputStream is = resource.getInputStream()) {
      DataInputStream dis = new DataInputStream(new BufferedInputStream(is, 64 * 1024));
      return new PcapReader(dis);

    } catch (Exception e) {
      log.error("Error while reading file", e);
    }

    throw new RuntimeException("Cannot create reader for file: " + file);
  }

  @Test
  public void testDynamicUpdateOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-dynamic-updates.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(68, pckts.size());
  }

  @Test
  public void testTCPStreamOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-multiple-dns-msg-over-tcp.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(5, pckts.size());

    long dnsMessages = pckts.stream().flatMap(p -> ((DNSPacket) p).getMessages().stream()).count();
    assertEquals(6, dnsMessages);
  }

  @Test
  public void testTCPAllMalformedStreamOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-tcp-all-malformed-packets.dups.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(0, pckts.size());
  }


  @Test
  public void testTCPIxfrOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-tcp-ixfr.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    List<Message> messages = pckts
        .stream()
        .flatMap(p -> ((DNSPacket) p).getMessages().stream())
        .collect(Collectors.toList());
    assertEquals(MessageType.QUERY, messages.get(0).getHeader().getQr());
    assertEquals(ResourceRecordType.IXFR, messages.get(0).getQuestions().get(0).getQType());

    assertEquals(MessageType.RESPONSE, messages.get(1).getHeader().getQr());
    assertEquals(ResourceRecordType.IXFR, messages.get(1).getQuestions().get(0).getQType());
  }

  @Test
  public void testUDP2DnsMessagesOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-udp-2-dns-ok.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    long count = pckts.stream().flatMap(p -> ((DNSPacket) p).getMessages().stream()).count();
    assertEquals(2, count);
  }


}

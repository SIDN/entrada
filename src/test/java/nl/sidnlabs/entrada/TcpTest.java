package nl.sidnlabs.entrada;

import static org.junit.Assert.assertEquals;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Packet;

public class TcpTest extends AbstractTest {

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
  public void testTCPStreamRetransmisionOk() {
    PcapReader reader =
        createReaderForComppressed("pcap/sidnlabs-test-tcp-stream-retransmision.pcap.gz");

    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(55, pckts.size());

    long dnsMessages = pckts
        .stream()
        .filter(p -> p != Packet.NULL)
        .flatMap(p -> ((DNSPacket) p).getMessages().stream())
        .count();
    assertEquals(1005, dnsMessages);

    long dnsQuery = pckts
        .stream()
        .flatMap(p -> ((DNSPacket) p).getMessages().stream())
        .filter(m -> m.getHeader().getQr() == MessageType.QUERY)
        .count();
    assertEquals(530, dnsQuery);

    long dnsResponse = pckts
        .stream()
        .flatMap(p -> ((DNSPacket) p).getMessages().stream())
        .filter(m -> m.getHeader().getQr() == MessageType.RESPONSE)
        .count();
    assertEquals(475, dnsResponse);

  }

  @Test
  public void testTCPStreamMultipleRequestOnlyOk() {
    PcapReader reader =
        createReaderFor("pcap/sidnlabs-tcp-stream-multiple-dns-message-request-only.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(8, pckts.size());

    long dnsMessages = pckts.stream().flatMap(p -> ((DNSPacket) p).getMessages().stream()).count();
    assertEquals(530, dnsMessages);

    long dnsQuery = pckts
        .stream()
        .flatMap(p -> ((DNSPacket) p).getMessages().stream())
        .filter(m -> m.getHeader().getQr() == MessageType.QUERY)
        .count();
    assertEquals(530, dnsQuery);
  }

  @Test
  public void testTCPStreamMultipleResponseOnlyOk() {
    PcapReader reader =
        createReaderFor("pcap/sidnlabs-tcp-stream-multiple-dns-message-response-only.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(47, pckts.size());

    long dnsMessages = pckts.stream().flatMap(p -> ((DNSPacket) p).getMessages().stream()).count();
    assertEquals(475, dnsMessages);

    long dnsQuery = pckts
        .stream()
        .flatMap(p -> ((DNSPacket) p).getMessages().stream())
        .filter(m -> m.getHeader().getQr() == MessageType.RESPONSE)
        .count();
    assertEquals(475, dnsQuery);
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
  public void testTCPPshFlagZeroBytesPayloadOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-tcp-psh-with-empty-payload.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(2, pckts.size());

    long count = pckts.stream().flatMap(p -> ((DNSPacket) p).getMessages().stream()).count();
    assertEquals(2, count);

    List<Message> messages = pckts
        .stream()
        .flatMap(p -> ((DNSPacket) p).getMessages().stream())
        .collect(Collectors.toList());

    assertEquals(MessageType.QUERY, messages.get(0).getHeader().getQr());
    assertEquals(MessageType.RESPONSE, messages.get(1).getHeader().getQr());
  }

  @Test
  public void testMultipleDNSInSingleTCPStreamOk() {
    PcapReader reader =
        createReaderFor("pcap/sidnlabs-test-multiple-dns-in-single-tcp-stream.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(4, pckts.size());

    long dnsMessages = pckts.stream().flatMap(p -> ((DNSPacket) p).getMessages().stream()).count();
    assertEquals(4, dnsMessages);
  }

  @Test
  public void testTCPOptionTsFoundOk() {
    PcapReader reader = createReaderFor("pcap/sidnlabs-test-tcp-ts-option.pcap");
    List<Packet> pckts = reader.stream().collect(Collectors.toList());
    assertEquals(1, pckts.size());

    // Packet p = pckts.get(0);
    // assertEquals(637077908, p.getTcpOptionTSval());
    // assertEquals(44805039, p.getTcpOptionTSecr());
  }


}

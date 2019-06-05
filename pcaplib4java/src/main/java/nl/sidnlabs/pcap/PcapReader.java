/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with ENTRADA. If
 * not, see [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidnlabs.pcap;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.codec.binary.Hex;
import com.google.common.collect.Multimap;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.pcap.decoder.DNSDecoder;
import nl.sidnlabs.pcap.decoder.ICMPDecoder;
import nl.sidnlabs.pcap.decoder.IPDecoder;
import nl.sidnlabs.pcap.decoder.TCPDecoder;
import nl.sidnlabs.pcap.decoder.UDPDecoder;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Datagram;
import nl.sidnlabs.pcap.packet.DatagramPayload;
import nl.sidnlabs.pcap.packet.ICMPPacket;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.TCPFlow;

/**
 * Read all data from a pcap file and decode all the packets
 *
 */
@Log4j2
public class PcapReader implements Iterable<Packet> {

  // needs no explanation
  public static final int DNS_PORT = 53;
  public static final long MAGIC_NUMBER = 0xA1B2C3D4;
  public static final int HEADER_SIZE = 24;
  public static final int PCAP_HEADER_LINKTYPE_OFFSET = 20;
  public static final int PACKET_HEADER_SIZE = 16;
  public static final int TIMESTAMP_OFFSET = 0;
  public static final int TIMESTAMP_MICROS_OFFSET = 4;
  public static final int CAP_LEN_OFFSET = 8;
  public static final int ETHERNET_HEADER_SIZE = 14;
  public static final int ETHERNET_TYPE_OFFSET = 12;
  public static final int ETHERNET_TYPE_IP = 0x800;
  public static final int ETHERNET_TYPE_IPV6 = 0x86dd;
  public static final int ETHERNET_TYPE_8021Q = 0x8100;
  public static final int ETHHERNET_MINIMUM_PYALOAD_SIZE = 46;
  public static final int SLL_HEADER_BASE_SIZE = 10; // SLL stands for Linux cooked-mode capture
  public static final int SLL_ADDRESS_LENGTH_OFFSET = 4; // relative to SLL header
  public static final int PROTOCOL_HEADER_SRC_PORT_OFFSET = 0;
  public static final int PROTOCOL_HEADER_DST_PORT_OFFSET = 2;
  public static final int PROTOCOL_TCP = 6;
  public static final int PROTOCOL_UDP = 17;
  public static final int PROTOCOL_FRAGMENTED = -1;

  public static final int TCP_DNS_LENGTH_PREFIX = 2;

  private DataInputStream is;
  private Iterator<Packet> iterator;
  private LinkType linkType;
  private boolean caughtEOF = false;
  // MathContext for BigDecimal to preserve only 16 decimal digits
  private MathContext matchCtx = new MathContext(16);

  // To read reversed-endian PCAPs; the header is the only part that switches
  private boolean reverseHeaderByteOrder = false;
  private int packetCounter;

  // metrics
  private int dnsDecodeError = 0;

  private IPDecoder ipDecoder = new IPDecoder();
  private ICMPDecoder icmpDecoder = new ICMPDecoder();
  private UDPDecoder udpDecoder = new UDPDecoder();
  private TCPDecoder tcpDecoder = new TCPDecoder();
  private DNSDecoder dnsDecoder = new DNSDecoder();

  public void init(DataInputStream is) throws IOException {
    this.is = is;
    iterator = new PacketIterator();

    byte[] pcapHeader = new byte[HEADER_SIZE];
    if (!readBytes(pcapHeader)) {
      //
      // This special check for EOF is because we don't want
      // PcapReader to barf on an empty file. This is the only
      // place we check caughtEOF.
      //
      if (caughtEOF) {
        log.warn("Skipping empty file");
        return;
      }
      throw new IOException("Couldn't read PCAP header");
    }

    if (!validateMagicNumber(pcapHeader))
      throw new IOException("Not a PCAP file (Couldn't find magic number)");

    long linkTypeVal =
        PcapReaderUtil.convertInt(pcapHeader, PCAP_HEADER_LINKTYPE_OFFSET, reverseHeaderByteOrder);
    if ((linkType = getLinkType(linkTypeVal)) == null)
      throw new IOException("Unsupported link type: " + linkTypeVal);
  }

  /**
   * Clear expired cache entries in order to avoid memory problems
   */
  public void clearCache(int tcpFlowCacheTimeout, int fragmentedIPcacheTimeout) {
    // clear tcp flows with expired packets
    List<TCPFlow> expiredList = new ArrayList<>();
    long now = System.currentTimeMillis();
    Multimap<TCPFlow, SequencePayload> flows = tcpDecoder.getFlows();
    for (TCPFlow flow : flows.keySet()) {
      Collection<SequencePayload> payloads = flows.get(flow);
      for (SequencePayload sequencePayload : payloads) {
        if ((sequencePayload.getTime() + tcpFlowCacheTimeout) <= now) {
          expiredList.add(flow);
          break;
        }
      }
    }

    // check IP datagrams
    List<Datagram> dgExpiredList = new ArrayList<>();

    for (Datagram dg : ipDecoder.getDatagrams().keySet()) {
      if ((dg.getTime() + fragmentedIPcacheTimeout) <= now) {
        dgExpiredList.add(dg);
      }
    }

    log.info("------------- Cache purge stats --------------");
    log.info("TCP flow cache size: " + flows.size());
    log.info("IP datagram cache size: " + ipDecoder.getDatagrams().size());
    log.info("Expired (to be removed) TCP flows: " + expiredList.size());
    log.info("Expired (to be removed) IP datagrams: " + dgExpiredList.size());
    log.info("----------------------------------------------------");

    // remove flows with expired packets
    for (TCPFlow tcpFlow : expiredList) {
      flows.removeAll(tcpFlow);
    }

    for (Datagram dg : dgExpiredList) {
      ipDecoder.getDatagrams().removeAll(dg);
    }

  }

  public void close() {
    try {
      is.close();
    } catch (IOException e) {
      log.error("Error closing inputstream", e);
    }
  }

  private Packet nextPacket() {
    byte[] pcapPacketHeader = new byte[PACKET_HEADER_SIZE];
    if (!readBytes(pcapPacketHeader)) {
      // no more data left
      return null;
    }

    long packetSize =
        PcapReaderUtil.convertInt(pcapPacketHeader, CAP_LEN_OFFSET, reverseHeaderByteOrder);
    byte[] packetData = new byte[(int) packetSize];

    if (!readBytes(packetData))
      return Packet.NULL;

    // find the start pos of the ip packet in the pcap frame
    int ipStart = findIPStart(packetData);

    if (ipStart == -1) {
      if (log.isDebugEnabled()) {
        log.debug(Hex.encodeHexString(packetData));
      }
      return Packet.NULL;
    }

    // decode the ip layer
    Packet packet = ipDecoder.decode(packetData, ipStart);
    if (packet == Packet.NULL) {
      if (log.isDebugEnabled()) {
        log.debug(Hex.encodeHexString(packetData));
      }
      // decode failed
      return packet;
    }

    // the pcap header for ervy packet contains a timestamp with the capture datetime of the packet
    long packetTimestamp =
        PcapReaderUtil.convertInt(pcapPacketHeader, TIMESTAMP_OFFSET, reverseHeaderByteOrder);
    packet.setTs(packetTimestamp);
    long packetTimestampMicros = PcapReaderUtil
        .convertInt(pcapPacketHeader, TIMESTAMP_MICROS_OFFSET, reverseHeaderByteOrder);
    packet.setTsmicros(packetTimestampMicros);

    // Prepare the timestamp with a BigDecimal to include microseconds
    BigDecimal packetTimestampUsec =
        new BigDecimal(packetTimestamp + (double) packetTimestampMicros / 1000000.0, matchCtx);
    packet.setTsUsec(packetTimestampUsec.doubleValue());

    int ipProtocolHeaderVersion = packet.getIpVersion();
    if (ipProtocolHeaderVersion == 4 || ipProtocolHeaderVersion == 6) {
      // list with payloads ready for decode
      byte[] tcpOrUdpPayload = new byte[0];

      /*
       * make sure there is no ethernet padding present. see: https://wiki.wireshark.org/Ethernet
       */
      if (packet.getTotalLength() < ETHHERNET_MINIMUM_PYALOAD_SIZE) { // 46
        // padding present, copy all data except the padding, to avoid problems decoding tcp/udp/dns
        packetData = Arrays.copyOfRange(packetData, 0, ipStart + packet.getTotalLength());
      }
      // check if the IP datagram is fragmented and needs to be reassembled
      packetData = ipDecoder.reassemble(packet, packetData, ipStart);
      // if decoder return empty byte array the IP packet is fragmented and is not the final
      // fragment
      if (packetData.length == 0) {
        return Packet.NULL;
      }
      // create a list with all the byte arrays that need to be decoded as dns packets
      List<byte[]> dnsBytes = new ArrayList<>();
      if ((ICMPDecoder.PROTOCOL_ICMP_V4 == packet.getProtocol())
          || (ICMPDecoder.PROTOCOL_ICMP_V6 == packet.getProtocol())) {
        // found icmp protocol
        ICMPPacket icmpPacket = (ICMPPacket) packet;
        icmpDecoder.reassemble(icmpPacket, ipStart, packetData);
        // do not process icmp packet further, because the dns packet might be corrupt (only 8 bytes
        // in icmp packet)
        packetCounter++;
        return icmpPacket;
      }

      if (PROTOCOL_TCP == packet.getProtocol()) {
        // found tcp protocol
        tcpOrUdpPayload = tcpDecoder
            .reassemble(packet, packet.getIpHeaderLen(), packetData.length, ipStart, packetData);
        /*
         * TCP flow may contain multiple dns messages break the TCP flow into the individual dns msg
         * blocks, every dns msg has a 2 byte msg prefix need at least the 2 byte len prefix to
         * start.
         */
        int tcpOrUdpPayloadIndex = 0;
        while ((tcpOrUdpPayload.length > TCP_DNS_LENGTH_PREFIX)
            && (tcpOrUdpPayloadIndex < tcpOrUdpPayload.length)) {
          byte[] lenBytes = new byte[2];
          System.arraycopy(tcpOrUdpPayload, tcpOrUdpPayloadIndex, lenBytes, 0, 2);
          int msgLen = PcapReaderUtil.convertShort(lenBytes);
          // add the 2byte msg len
          tcpOrUdpPayloadIndex += 2;
          if ((tcpOrUdpPayloadIndex + msgLen) <= tcpOrUdpPayload.length) {
            byte[] msgBytes = new byte[msgLen];
            System.arraycopy(tcpOrUdpPayload, tcpOrUdpPayloadIndex, msgBytes, 0, msgLen);
            dnsBytes.add(msgBytes);
            // add the msg len to the index
            tcpOrUdpPayloadIndex += msgLen;
          } else {
            // invalid msg len
            if (log.isDebugEnabled()) {
              log
                  .debug(
                      "Invalid TCP payload length, msgLen= " + msgLen + " tcpOrUdpPayload.length= "
                          + tcpOrUdpPayload.length + " ack=" + packet.isTcpFlagAck());
            }
            break;
          }
        }
        if (log.isDebugEnabled() && dnsBytes.size() > 1) {
          log.debug("multiple msg in TCP stream");
        }
      } else if (PROTOCOL_UDP == packet.getProtocol()) {
        // found UDP protocol
        tcpOrUdpPayload = udpDecoder
            .reassemble(packet, packet.getIpHeaderLen(), packetData.length, ipStart, packetData);
        dnsBytes.add(tcpOrUdpPayload);
      }

      if (packet.getFragOffset() == 0 && packet.getSrcPort() != PcapReader.DNS_PORT
          && packet.getDstPort() != PcapReader.DNS_PORT) {
        // not a dns packet
        if (log.isDebugEnabled()) {
          log.debug("NON DNS protocol: " + packet);
        }
        return Packet.NULL;
      }

      if (dnsBytes == null || dnsBytes.isEmpty()) {
        // no DNS packets found
        return Packet.NULL;
      }

      // only dns packets make it to here.
      packetCounter++;
      DNSPacket dnsPacket = (DNSPacket) packet;
      try {
        dnsDecoder.decode(dnsPacket, dnsBytes);
      } catch (Exception e) {
        /*
         * catch anything which might get thrown out of the dns decoding if the tcp bytes are
         * somehow incorrectly assembled the dns decoder will fail.
         * 
         * ignore the error and skip the packet.
         */
        if (log.isDebugEnabled()) {
          log.debug("Packet payload could not be decoded (malformed packet?) details: " + packet);
        }
        dnsDecodeError++;
      }

      if (dnsPacket.getMessages() == null || dnsPacket.getMessageCount() == 0) {
        // no dns message(s) found
        return Packet.NULL;
      }
    }
    return packet;
  }

  protected boolean validateMagicNumber(byte[] pcapHeader) {
    if (PcapReaderUtil.convertInt(pcapHeader) == MAGIC_NUMBER) {
      return true;
    } else if (PcapReaderUtil.convertInt(pcapHeader, true) == MAGIC_NUMBER) {
      reverseHeaderByteOrder = true;
      return true;
    } else {
      return false;
    }
  }

  protected enum LinkType {
    NULL, EN10MB, RAW, LOOP, LINUX_SLL
  }

  protected LinkType getLinkType(long linkTypeVal) {
    switch ((int) linkTypeVal) {
      case 0:
        return LinkType.NULL;
      case 1:
        return LinkType.EN10MB;
      case 101:
        return LinkType.RAW;
      case 108:
        return LinkType.LOOP;
      case 113:
        return LinkType.LINUX_SLL;
      default:
        return null;
    }
  }

  protected int findIPStart(byte[] packet) {
    int start = -1;
    switch (linkType) {
      case NULL:
        return 4;
      case EN10MB:
        start = ETHERNET_HEADER_SIZE;
        int etherType = PcapReaderUtil.convertShort(packet, ETHERNET_TYPE_OFFSET);
        if (etherType == ETHERNET_TYPE_8021Q) {
          etherType = PcapReaderUtil.convertShort(packet, ETHERNET_TYPE_OFFSET + 4);
          start += 4;
        }
        if (etherType == ETHERNET_TYPE_IP || etherType == ETHERNET_TYPE_IPV6)
          return start;
        break;
      case RAW:
        return 0;
      case LOOP:
        return 4;
      case LINUX_SLL:
        start = SLL_HEADER_BASE_SIZE;
        int sllAddressLength = PcapReaderUtil.convertShort(packet, SLL_ADDRESS_LENGTH_OFFSET);
        start += sllAddressLength;
        return start;
    }
    return -1;
  }



  protected boolean readBytes(byte[] buf) {
    try {
      is.readFully(buf);
      return true;
    } catch (EOFException e) {
      // Reached the end of the stream
      caughtEOF = true;
      return false;
    } catch (IOException e) {
      log.error("Error while reading " + buf.length + " bytes from buffer");
      return false;
    }
  }

  @Override
  public Iterator<Packet> iterator() {
    return iterator;
  }


  public Multimap<TCPFlow, SequencePayload> getFlows() {
    return tcpDecoder.getFlows();
  }

  public void setFlows(Multimap<TCPFlow, SequencePayload> flows) {
    tcpDecoder.setFlows(flows);
  }

  private class PacketIterator implements Iterator<Packet> {
    private Packet next;

    private void fetchNext() {
      if (next == null) {
        // skip fragmented packets until they are assembled
        do {
          try {
            next = nextPacket();
          } catch (Exception e) {
            log.error("PCAP decode error: ", e);
            next = Packet.NULL;
          }
        } while (next == Packet.NULL);
      }
    }

    @Override
    public boolean hasNext() {
      // fetchNext will keep result in "next" var so that when next() is
      // called the data does not have to be parsed a 2nd time
      fetchNext();
      if (next != null)
        return true;

      // no more data left
      int remainingFlows = tcpDecoder.getFlows().size() + ipDecoder.getDatagrams().size();
      if (remainingFlows > 0) {
        log.warn("Still " + remainingFlows + " flows queued. Missing packets to finish assembly?");
        log.warn("Packets processed: " + packetCounter);
        log.warn("Messages decoded:  " + dnsDecoder.getMessageCounter());
      }

      return false;
    }

    @Override
    public Packet next() {
      fetchNext();

      if (next == null) {
        throw new NoSuchElementException("No more packets to decode");
      }
      try {
        return next;
      } finally {
        // make sure to set next to null so the next packet is read from the stream
        next = null;
      }
    }

    @Override
    public void remove() {
      // Not supported
    }
  }

  public int getTcpPrefixError() {
    return tcpDecoder.getTcpPrefixError();
  }

  public int getDnsDecodeError() {
    return dnsDecodeError;
  }

  public Multimap<Datagram, DatagramPayload> getDatagrams() {
    return ipDecoder.getDatagrams();
  }

  public void setDatagrams(Multimap<Datagram, DatagramPayload> map) {
    ipDecoder.setDatagrams(map);
  }

}

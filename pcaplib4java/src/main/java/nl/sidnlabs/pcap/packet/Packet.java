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
package nl.sidnlabs.pcap.packet;

import lombok.Data;
import nl.sidnlabs.pcap.decoder.IPDecoder;

/**
 * Packet contains a combination of IP layer and UDP/TCP/DNS layer data Fragmented IP is joined into
 * a single Packet object Fragmented UDP is joined into a single Packet object TCP session with
 * multiple DNS queries in a stream before the PSH or FIN will cause multiple DNS messages to get
 * added to the Packet object.
 * 
 */
@Data
public class Packet {

  public static final Packet NULL = new Packet();

  // network
  protected int len;
  protected long ts;
  protected long tsmicros;
  protected double tsUsec;
  // ip
  protected long ipId;
  protected int ttl;
  protected byte ipVersion;
  protected int ipHeaderLen;
  protected short protocol;
  protected String src;
  protected String dst;
  protected long fragOffset;
  protected boolean fragmented;
  protected boolean lastFragment;
  // ip fragments
  protected int reassembledFragments;
  // ipv6
  protected boolean fragmentFlagM;
  // udp tcp
  protected int reassembledTCPFragments;
  protected int srcPort;
  protected int dstPort;
  protected int tcpflow;
  // udp
  protected int udpsum;
  protected int udpLength;
  // tcp
  protected int tcpHeaderLen;
  protected long tcpSeq;
  protected long tcpAck;
  protected boolean tcpFlagNs;
  protected boolean tcpFlagCwr;
  protected boolean tcpFlagEce;
  protected boolean tcpFlagUrg;
  protected boolean tcpFlagAck;
  protected boolean tcpFlagPsh;
  protected boolean tcpFlagRst;
  protected boolean tcpFlagSyn;
  protected boolean tcpFlagFin;
  protected int tcpWindowSize;

  private int totalLength;
  protected int payloadLength;

  public TCPFlow getFlow() {
    return new TCPFlow(src, srcPort, dst, dstPort, protocol);
  }

  public Datagram getDatagram() {
    return new Datagram(getSrc(), getDst(), getIpId(), String.valueOf(getProtocol()),
        System.currentTimeMillis());
  }


  public boolean isIPv4() {
    return getIpVersion() == IPDecoder.IP_PROTOCOL_VERSION_4;
  }

  public boolean isIPv6() {
    return getIpVersion() == IPDecoder.IP_PROTOCOL_VERSION_6;
  }
}

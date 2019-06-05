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
package nl.sidnlabs.pcap.decoder;

import java.util.List;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.util.NetworkData;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;

/**
 * Decode the dns payload of an UDP or TCP message
 *
 */
@Data
@Log4j2
public class DNSDecoder {

  private int dnsDecodeError;
  private int messageCounter;


  public int decode(DNSPacket packet, List<byte[]> payloads) {
    int counter = 0;
    for (byte[] payload : payloads) {
      decode(packet, payload);
      counter++;
    }
    return counter;
  }


  public void decode(DNSPacket packet, byte[] payload) {
    decode(packet, payload, false);
  }

  public void decode(DNSPacket packet, byte[] payload, boolean allowFaill) {

    NetworkData nd = null;
    Message dnsMessage = null;

    nd = new NetworkData(payload);
    try {
      dnsMessage = new Message(nd, allowFaill);
      dnsMessage.setBytes(nd.length());
      packet.pushMessage(dnsMessage);
      messageCounter++;
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("error decoding maybe corrupt packet: " + packet, e);
      }
      dnsDecodeError++;
    }

    if (log.isDebugEnabled() && packet.getProtocol() == PcapReader.PROTOCOL_UDP
        && nd.isBytesAvailable()) {
      log
          .debug("udp padding found for: " + packet.getSrc() + " " + packet.getSrcPort()
              + " pad bytes: " + (nd.length() - nd.getReaderIndex()));
    }

  }

  public void reset() {
    dnsDecodeError = 0;
    messageCounter = 0;
  }

}

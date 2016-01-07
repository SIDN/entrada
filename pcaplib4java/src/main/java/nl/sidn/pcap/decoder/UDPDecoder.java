/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].
 *
 */	
package nl.sidn.pcap.decoder;

import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.PcapReaderUtil;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.util.UDPUtil;

/**
 * Decode UDP packets
 *
 */
public class UDPDecoder {

	/**
	 * Decode the udp packet, supports reassembly of fragmented packets
	 * @param packet
	 * @param ipHeaderLen
	 * @param totalLength
	 * @param ipStart
	 * @param packetData
	 * @return payload bytes or null if not a valid packet
	 */
	public byte[] reassemble(Packet packet, int ipHeaderLen,int totalLength, int ipStart, byte[] packetData){
		//if the offset == 0 then the payload contains the udp header, do not read the header, only get the udp payload bytes
		
		packet.setSrcPort(PcapReaderUtil.convertShort(packetData, ipStart + ipHeaderLen + PcapReader.PROTOCOL_HEADER_SRC_PORT_OFFSET));
		packet.setDstPort( PcapReaderUtil.convertShort(packetData, ipStart + ipHeaderLen + PcapReader.PROTOCOL_HEADER_DST_PORT_OFFSET));

		if (packet.getIpVersion() == 4) {
			int cksum = UDPUtil.getUdpChecksum(packetData, ipStart, ipHeaderLen);
			if (cksum >= 0)
			packet.setUdpsum(cksum);
		}

		int payloadDataStart = ipStart + ipHeaderLen + UDPUtil.UDP_HEADER_SIZE;
		int payloadLength = totalLength - ipHeaderLen - UDPUtil.UDP_HEADER_SIZE;
		byte[] packetPayload = PcapReaderUtil.readPayload(packetData, payloadDataStart, payloadLength);
			
		//total length of packet, might be wrong if icmp truncation is in play
		packet.setUdpLength(totalLength);
        packet.setPayloadLength(UDPUtil.getUdpLen(packetData, ipStart, ipHeaderLen));
		
		
		if (packet.getFragOffset() == 0 && packet.getSrcPort() != PcapReader.DNS_PORT && packet.getDstPort() != PcapReader.DNS_PORT){
			//not a dns packet
			return null;
		}
		
		return packetPayload;
	}

}

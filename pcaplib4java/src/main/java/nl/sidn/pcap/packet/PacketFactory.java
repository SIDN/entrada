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
package nl.sidn.pcap.packet;

import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.decoder.ICMPDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Create a packet object based on the protocol number.
 *
 */
public class PacketFactory {

	public static final Log LOG = LogFactory.getLog(PacketFactory.class);

	public static Packet create(int protocol) {
		Packet p = null;
		if ((protocol == ICMPDecoder.PROTOCOL_ICMP_V4) || (protocol == ICMPDecoder.PROTOCOL_ICMP_V6)) {
			p = new ICMPPacket();
		} else if ((protocol == PcapReader.PROTOCOL_UDP) || (protocol == PcapReader.PROTOCOL_TCP)) {
			p = new DNSPacket();
		} else {
			return Packet.NULL;
		}

		p.setProtocol((short) protocol);
		return p;
	}

}

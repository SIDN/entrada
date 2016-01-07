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
package nl.sidn.pcap.util;

import nl.sidn.pcap.PcapReaderUtil;

public class UDPUtil {

	public static final int UDP_HEADER_SIZE = 8;
	public static final int UDP_HEADER_LEN_OFFSET = 4;

	public static byte[] extractPayload(byte[] packetData) {
		int length = packetData.length - UDP_HEADER_SIZE;
		byte[] data = new byte[length];
		System.arraycopy(packetData, UDP_HEADER_SIZE, data, 0, length);
		return data;
	}

	/**
	 * Get size of udp packet payload
	 * @param packetData
	 * @param ipStart
	 * @param ipHeaderLen
	 * @return
	 */
	public static int getUdpLen(byte[] packetData, int ipStart, int ipHeaderLen) {
		return PcapReaderUtil.convertShort(packetData, ipStart+ipHeaderLen+UDP_HEADER_LEN_OFFSET) - UDP_HEADER_SIZE;
	}
	
	public static int getUdpChecksum(byte[] packetData, int ipStart, int ipHeaderLen) {
		/*
		 * No Checksum on this packet?
		 */
		if (packetData[ipStart + ipHeaderLen + 6] == 0 &&
		    packetData[ipStart + ipHeaderLen + 7] == 0)
			return -1;

		/*
		 * Build data[] that we can checksum.  Its a pseudo-header
		 * followed by the entire UDP packet.
		 */
		byte data[] = new byte[packetData.length - ipStart - ipHeaderLen + 12];
		int sum = 0;
		System.arraycopy(packetData, ipStart + IPv4Util.IP_SRC_OFFSET, data, 0, 4);
		System.arraycopy(packetData, ipStart + IPv4Util.IP_DST_OFFSET, data, 4, 4);
		data[8] = 0;
		data[9] = 17;	/* IPPROTO_UDP */
		System.arraycopy(packetData, ipStart + ipHeaderLen + 4,    data, 10, 2);
		System.arraycopy(packetData, ipStart + ipHeaderLen,        data, 12, packetData.length - ipStart - ipHeaderLen);
		for (int i = 0; i<data.length; i++) {
			int j = data[i];
			if (j < 0)
				j += 256;
			sum += j << (i % 2 == 0 ? 8 : 0);
		}
		sum = (sum >> 16) + (sum & 0xffff);
		sum += (sum >> 16);
		return (~sum) & 0xffff;
	}
}

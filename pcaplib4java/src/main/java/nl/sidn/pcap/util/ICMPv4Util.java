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

public class ICMPv4Util {
	
	public static final int ICMP_TYPE_OFFSET = 0;
	public static final int ICMP_CODE_OFFSET = 1;
	public static final int ICMP_REST_OF_HDR_OFFSET = 4;
	public static final int ICMP_REST_OF_HDR_LEN = 4;
	
	public static final int ICMP_DATA_OFFSET = 8;
	public static final int ICMP_HDR_LEN = 8;
	
	public static final int ICMP_ECHO_REQUEST_ID_OFFSET = 4;
	public static final int ICMP_ECHO_REQUEST_SEQ_OFFSET = 6;
	public static final int ICMP_ECHO_REQUEST_PAYLOAD_OFFSET = 8;
	
	public static short decodeType(byte[] packetData){
	   return PcapReaderUtil.readUnsignedByte(packetData, ICMP_TYPE_OFFSET);
	}
	
	public static short decodeCode(byte[] packetData){
	   return PcapReaderUtil.readUnsignedByte(packetData, ICMP_CODE_OFFSET);
	}
	
	public static byte[] extractRestOfHeader(byte[] packetData){
		byte[] data = new byte[4];
		System.arraycopy(packetData, ICMP_REST_OF_HDR_OFFSET, data, 0, ICMP_REST_OF_HDR_LEN);
		return data;
	}
	
	public static byte[] extractPayload(byte[] packetData){
		int length = packetData.length - ICMP_HDR_LEN;
		byte[] data = new byte[length];
		System.arraycopy(packetData, ICMP_DATA_OFFSET, data, 0, length);
		return data;
	}
	
	public static byte[] extractEchoRequestPayload(byte[] packetData){
		byte[] data = new byte[packetData.length-ICMP_ECHO_REQUEST_PAYLOAD_OFFSET];
		System.arraycopy(packetData, ICMP_ECHO_REQUEST_PAYLOAD_OFFSET, data, 0, data.length);
		return data;
	}
}

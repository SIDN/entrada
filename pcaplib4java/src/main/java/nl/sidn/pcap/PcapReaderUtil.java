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
package nl.sidn.pcap;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PcapReaderUtil {
	public static final Log LOG = LogFactory.getLog(PcapReaderUtil.class);
	private static Map<Integer, String> protocols;

	public static long convertInt(byte[] data) {
		return convertInt(data, false);
	}
	
	public static long convertInt(byte[] data, boolean reversed) {
		if (!reversed)
		{
			return ((data[3] & 0xFF) << 24) | ((data[2] & 0xFF) << 16)
					| ((data[1] & 0xFF) << 8) | (data[0] & 0xFF);
		}
		else
		{
			return ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16)
					| ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
		}
	}

	public static long convertInt(byte[] data, int offset, boolean reversed) {
		byte[] target = new byte[4];
		System.arraycopy(data, offset, target, 0, target.length);
		return convertInt(target, reversed);
	}
	
	public static long convertInt(byte[] data, int offset) {
		byte[] target = new byte[4];
		System.arraycopy(data, offset, target, 0, target.length);
		return convertInt(target, false);
	}

	public static int convertShort(byte[] data) {
		return ((data[0] & 0xFF) << 8) | (data[1] & 0xFF);
	}

	public static byte[] convertShort(int data) {
		byte[] result = new byte[2];
		result[0] = (byte)(data >> 8);
		result[1] = (byte)(data);
		return result;
	}

	public static int convertShort(byte[] data, int offset) {
		byte[] target = new byte[2];
		System.arraycopy(data, offset, target, 0, target.length);
		return convertShort(target);
	}

	//A java workaround for header fields like seq/ack which are ulongs --M
	public static long convertUnsignedInt(byte[] data,int offset) {
		byte[] target = new byte[4];
		System.arraycopy(data, offset, target, 0, target.length);

		BigInteger placeholder = new BigInteger(1,target);
		return placeholder.longValue();
	}

	public static String convertProtocolIdentifier(int identifier) {
		return protocols.get(identifier);
	}

	public static String convertAddress(byte[] data, int offset, int size) {
		byte[] addr = new byte[size];
		System.arraycopy(data, offset, addr, 0, addr.length);
		try {
			return InetAddress.getByAddress(addr).getHostAddress();
		} catch (UnknownHostException e) {
			LOG.error("Ivalid host address: ",e);
			return null;
		}
	}
	
	public static short readUnsignedByte(byte[] buf, int index){
		int byte1 = (0xFF & buf[index]);
		return (short)byte1;
	}
	
	/**
	 * Reads the packet payload and returns it as byte[].
	 * If the payload could not be read an empty byte[] is returned.
	 * @param packetData
	 * @param payloadDataStart
	 * @return payload as byte[]
	 */
	public static byte[] readPayload(byte[] packetData, int payloadDataStart, int payloadLength) {
		if (payloadLength < 0) {
			LOG.warn("Malformed packet - negative payload length. Returning empty payload.");
			return new byte[0];
		}
		if (payloadDataStart > packetData.length) {
			LOG.warn("Payload start (" + payloadDataStart + ") is larger than packet data (" + packetData.length + "). Returning empty payload.");
			return new byte[0];
		}
		if (payloadDataStart + payloadLength > packetData.length) {
			payloadLength = packetData.length - payloadDataStart;
		}
		byte[] data = new byte[payloadLength];
		System.arraycopy(packetData, payloadDataStart, data, 0, payloadLength);
		return data;
	}
}

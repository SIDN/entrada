package nl.sidn.pcap.util;

import nl.sidn.pcap.PcapReaderUtil;

public class ICMPv6Util {
	
	public static final int ICMP_TYPE_OFFSET = 0;
	public static final int ICMP_CODE_OFFSET = 1;
	
	//for icmp error messages the hdr is 8 bytes
	public static final int ICMP_DATA_OFFSET = 8;
	public static final int ICMP_HDR_LEN = 8;
	
	public static short decodeType(byte[] packetData){
	   return PcapReaderUtil.readUnsignedByte(packetData, ICMP_TYPE_OFFSET);
	}
	
	public static short decodeCode(byte[] packetData){
	   return PcapReaderUtil.readUnsignedByte(packetData, ICMP_CODE_OFFSET);
	}
	
	public static byte[] extractPayload(byte[] packetData){
		int length = packetData.length - ICMP_HDR_LEN;
		byte[] data = new byte[length];
		System.arraycopy(packetData, ICMP_DATA_OFFSET, data, 0, length);
		return data;
	}
}

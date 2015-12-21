package nl.sidn.pcap.util;

import nl.sidn.pcap.PcapReaderUtil;

public class IPv4Util {
	
	public static final int IP_PROTOCOL_OFFSET = 9;	// relative to start of IP header
	public static final int IP_SRC_OFFSET = 12;	// relative to start of IP header
	
	public static final int IP_VHL_OFFSET = 0;	// relative to start of IP header
	public static final int IP_TTL_OFFSET = 8;	// relative to start of IP header	
	
	public static final int IP_DST_OFFSET = 16;	// relative to start of IP header
	public static final int IP_ID_OFFSET = 4;

	public static int decodeTTL(byte[] packetData, int ipStart){
		return packetData[ipStart + IP_TTL_OFFSET] & 0xFF;
	}
	
	public static int decodeProtocol(byte[] packetData, int ipStart){
		return packetData[ipStart + IP_PROTOCOL_OFFSET];
	}
	
	public static String decodeSrc(byte[] packetData, int ipStart){
		return PcapReaderUtil.convertAddress(packetData, ipStart + IP_SRC_OFFSET, 4);
	}

	public static String decodeDst(byte[] packetData, int ipStart){
		return PcapReaderUtil.convertAddress(packetData, ipStart + IP_DST_OFFSET, 4);
	}
	
	public static int decodeId(byte[] packetData, int ipStart){
		return PcapReaderUtil.convertShort(packetData, ipStart + IP_ID_OFFSET);
	}

	public static  int getInternetProtocolHeaderVersion(byte[] packet, int ipStart) {
		return (packet[ipStart + IP_VHL_OFFSET] >> 4) & 0xF;
	}
	
	public static int getInternetProtocolHeaderLength(byte[] packet, int ipStart) {
		return (packet[ipStart + IP_VHL_OFFSET] & 0xF) * 4;
	}

}

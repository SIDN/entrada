package nl.sidn.pcap.packet;

import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.decoder.ICMPDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Create a packet object based on the protocol number.
 * @author maarten
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

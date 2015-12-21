package nl.sidn.pcap.packet;

import java.util.ArrayList;
import java.util.List;

import nl.sidn.dnslib.message.Message;

public class DNSPacket extends Packet {

	// dns messages
	private List<Message> messages = new ArrayList<>();

	
	/**
	 * get dns message from packet
	 * @return first message in list, null if no message was found
	 */
	public Message getMessage() {
		if (messages.size() > 0) {
			return messages.get(0);
		}
		return null;
	}

	/**
	 * remove dns message from packet
	 * @return message that is removed, null if no message was found
	 */
	public Message popMessage() {
		if (messages.size() > 0) {
			return messages.remove(0);
		}
		return null;
	}

	public void pushMessage(Message msg) {
		messages.add(msg);
	}

	public void setMessage(Message message) {
		// this.message = message;
		messages.add(message);
	}

	public int getMessageCount() {
		return messages.size();
	}

	public List<Message> getMessages() {
		return messages;
	}

	@Override
	public String toString() {
		return "DNSPacket [messages=" + messages + ", len=" + len + ", ts="
				+ ts + ", tsmicros=" + tsmicros + ", tsUsec=" + tsUsec
				+ ", ipId=" + ipId + ", ttl=" + ttl + ", ipVersion="
				+ ipVersion + ", ipHeaderLen=" + ipHeaderLen + ", protocol="
				+ protocol + ", src=" + src + ", dst=" + dst + ", srcPort="
				+ srcPort + ", dstPort=" + dstPort + ", tcpflow=" + tcpflow
				+ ", udpsum=" + udpsum + ", udpLength=" + udpLength
				+ ", tcpHeaderLen=" + tcpHeaderLen + ", tcpSeq=" + tcpSeq
				+ ", tcpAck=" + tcpAck + ", tcpFlagNs=" + tcpFlagNs
				+ ", tcpFlagCwr=" + tcpFlagCwr + ", tcpFlagEce=" + tcpFlagEce
				+ ", tcpFlagUrg=" + tcpFlagUrg + ", tcpFlagAck=" + tcpFlagAck
				+ ", tcpFlagPsh=" + tcpFlagPsh + ", tcpFlagRst=" + tcpFlagRst
				+ ", tcpFlagSyn=" + tcpFlagSyn + ", tcpFlagFin=" + tcpFlagFin
				+ ", tcpWindowSize=" + tcpWindowSize
				+ ", reassembledFragments=" + reassembledFragments + "]";
	}

}

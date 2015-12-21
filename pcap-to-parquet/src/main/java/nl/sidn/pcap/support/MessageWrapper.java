package nl.sidn.pcap.support;

import nl.sidn.dnslib.message.Message;
import nl.sidn.pcap.packet.Packet;

public class MessageWrapper {

	private Message message;
	private Packet packet;

	public MessageWrapper() {
	}

	public MessageWrapper(Message message, Packet packet) {
		this.message = message;
		this.packet = packet;

	}

	public Message getMessage() {
		return message;
	}

	public Packet getPacket() {
		return packet;
	}

}

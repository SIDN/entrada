package nl.sidn.pcap.support;

import nl.sidn.dnslib.message.Message;
import nl.sidn.pcap.packet.Packet;

public class PacketCombination {

	public static final PacketCombination NULL = new PacketCombination(null,null, null);

	private String server;
	private Packet request;
	private Message requestMessage;
	private Packet response;
	private Message responseMessage;

	public PacketCombination(Packet request, Message requestMessage, String server) {
		this(request, requestMessage, server, null, null);
	}

	public PacketCombination(Packet request, Message requestMessage, String server, Packet response, Message responseMessage) {
		this.request = request;
		this.response = response;
		this.requestMessage = requestMessage;
		this.responseMessage = responseMessage;
		this.server = server;
	}

	public Packet getRequest() {
		return request;
	}

	public Packet getResponse() {
		return response;
	}

	public Message getRequestMessage() {
		return requestMessage;
	}

	public Message getResponseMessage() {
		return responseMessage;
	}

	public String getServer() {
		return server;
	}

}

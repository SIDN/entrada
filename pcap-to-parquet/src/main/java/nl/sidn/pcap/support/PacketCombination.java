/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].
 *
 */	
package nl.sidn.pcap.support;

import nl.sidn.dnslib.message.Message;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.util.Settings.ServerInfo;

public class PacketCombination {

	public static final PacketCombination NULL = new PacketCombination(null,null, null, false);

	private ServerInfo server;
	private Packet request;
	private Message requestMessage;
	private Packet response;
	private Message responseMessage;
	//true if this packet has expired from cache
	private boolean expired;

	public PacketCombination(Packet request, Message requestMessage, ServerInfo server, boolean expired) {
		this(request, requestMessage, server, null, null, expired);
	}

	public PacketCombination(Packet request, Message requestMessage, ServerInfo server, Packet response, Message responseMessage, boolean expired) {
		this.request = request;
		this.response = response;
		this.requestMessage = requestMessage;
		this.responseMessage = responseMessage;
		this.server = server;
		this.expired = expired;
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

	public ServerInfo getServer() {
		return server;
	}
	
	public boolean isExpired(){
		return expired;
	}

}

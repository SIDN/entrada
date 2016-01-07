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

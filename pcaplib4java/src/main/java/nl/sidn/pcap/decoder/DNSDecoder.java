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
package nl.sidn.pcap.decoder;

import java.util.List;

import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.util.NetworkData;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.packet.DNSPacket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Decode the dns payload of an UDP or TCP message
 *
 */
public class DNSDecoder {
	public static final Log LOG = LogFactory.getLog(DNSDecoder.class);
	
	private int dnsDecodeError;
	private int messageCounter;
	
	
	public int decode(DNSPacket packet, List<byte[]> payloads) {
		int counter = 0;
		for (byte[] payload : payloads) {
			decode(packet,payload);
			counter++;
		}
		return counter;
	}
	
	
	public void decode(DNSPacket packet, byte[] payload) {
		decode(packet, payload, false);
	}
	
	public void decode(DNSPacket packet, byte[] payload, boolean allowFaill) {
	
		NetworkData nd = null;
		Message dnsMessage = null;

		nd = new NetworkData(payload);
		try {
			dnsMessage = new Message(nd, allowFaill);
		} catch (Exception e) {
			if(LOG.isDebugEnabled()){
				LOG.info("error decoding maybe corrupt packet: " + packet, e);
			}else{
				LOG.info("error decoding maybe corrupt packet, message: " + e.getMessage());
			}
			dnsDecodeError++;
		}
		dnsMessage.setBytes(payload.length);
		packet.pushMessage(dnsMessage);
		messageCounter++;
				
		if(LOG.isDebugEnabled() && packet.getProtocol() == PcapReader.PROTOCOL_UDP && nd.isBytesAvailable()){
			LOG.debug("udp padding found for: " + packet.getSrc() + " " + packet.getSrcPort() + " pad bytes: " + (nd.getNumBytes() - nd.getReaderIndex()) );
		}

	}

	public int getDnsDecodeError() {
		return dnsDecodeError;
	}

	public int getMessageCounter() {
		return messageCounter;
	}
	
	public void reset(){
		dnsDecodeError = 0;
		messageCounter = 0;
	}

}

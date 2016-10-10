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

import java.util.Arrays;
import java.util.Collection;

import nl.sidn.pcap.PcapReaderUtil;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.packet.PacketFactory;
import nl.sidn.pcap.util.IPv4Util;
import nl.sidn.pcap.util.IPv6Util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Bytes;

/**
 * Decode the IP header
 *
 */
public class IPDecoder{
	
	public static final Log LOG = LogFactory.getLog(PacketFactory.class);

	public static final int IP_PROTOCOL_VERSION_4 = 4;
	public static final int IP_PROTOCOL_VERSION_6 = 6;
	public static final int IP_TOTAL_LEN_OFFSET = 2;	// relative to start of IP header
	public static final int IP_FLAGS = 6;
	public static final int IP_FRAGMENT_OFFSET = 6; //The first 3 bits are the flags
	
	private Multimap<nl.sidn.pcap.packet.Datagram, nl.sidn.pcap.packet.DatagramPayload> datagrams = TreeMultimap.create();
	

	public Packet decode(byte[] packetData, int ipStart){

		if (ipStart == -1)
			return Packet.NULL;
		
		int ipProtocolHeaderVersion = IPv4Util.getInternetProtocolHeaderVersion(packetData, ipStart);
		int protocol = -1;
		
		if (ipProtocolHeaderVersion == IP_PROTOCOL_VERSION_4){
			protocol = IPv4Util.decodeProtocol(packetData, ipStart);
		}else if(ipProtocolHeaderVersion == IP_PROTOCOL_VERSION_6){
			protocol = IPv6Util.decodeProtocol(packetData, ipStart);
		}else{
			LOG.error("Unsupported IP version " + ipProtocolHeaderVersion  + " ipstart=" + ipStart );
			return Packet.NULL;
		}
		Packet packet = PacketFactory.create(protocol);
		if(packet == Packet.NULL){
			//no supported protocol skip packet
			if(LOG.isDebugEnabled()){
				LOG.debug("Invalid protocol " + protocol);
			}
			return packet;
		}
		
		packet.setIpVersion( (byte)ipProtocolHeaderVersion);
		
		int totalLength = 0;
		if (ipProtocolHeaderVersion == IP_PROTOCOL_VERSION_4) {
			int ipHeaderLen = IPv4Util.getInternetProtocolHeaderLength(packetData, ipStart);
			packet.setIpHeaderLen(ipHeaderLen);
			
			buildInternetProtocolV4Packet(packet, packetData, ipStart);
			totalLength = PcapReaderUtil.convertShort(packetData, ipStart + IP_TOTAL_LEN_OFFSET);
			decodeV4Fragmented(packet,ipStart, packetData);
		} else if (ipProtocolHeaderVersion == IP_PROTOCOL_VERSION_6) {
			int ipHeaderLen = IPv6Util.getInternetProtocolHeaderLength(packetData, ipStart);
			packet.setIpHeaderLen(ipHeaderLen);
			
			buildInternetProtocolV6Packet(packet, packetData, ipStart);
			int payloadLength = PcapReaderUtil.convertShort(packetData, ipStart + IPv6Util.IPV6_PAYLOAD_LEN_OFFSET);
			totalLength = payloadLength + IPv6Util.IPV6_HEADER_SIZE;
			
			decodeV6Fragmented(packet, ipStart, packetData);
			//v6 last frag is field in extension header
			if(packet.isFragmented()){
				IPv6Util.buildInternetProtocolV6ExtensionHeaderFragment(packet,packetData,ipStart);
			}
		}
		
		packet.setTotalLength(totalLength);	
		
		return packet;
	}
	
	private void decodeV6Fragmented(Packet packet,int ipStart, byte[] packetData){
		//assumption that the first extension header is the fragmentation header
		int nxtHdr = packetData[ipStart + IPv6Util.IPV6_NEXTHEADER_OFFSET];
		packet.setFragmented(nxtHdr == IPv6Util.IPV6_FRAGMENT_EXTENTION_TYPE);
	}
	
	private void decodeV4Fragmented(Packet packet, int ipStart, byte[] packetData){
		long fragmentOffset = (PcapReaderUtil.convertShort(packetData, ipStart + IP_FRAGMENT_OFFSET) & 0x1FFF) * 8;
		packet.setFragOffset(fragmentOffset);
		
		int flags = packetData[ipStart + IP_FLAGS] & 0xE0;

		if ((flags & 0x20) != 0 || fragmentOffset != 0) {
			packet.setFragmented(true);
			packet.setLastFragment(((flags & 0x20) == 0 && fragmentOffset != 0));
		} else {
			packet.setFragmented(false);
		}
	}

	
	private void buildInternetProtocolV4Packet(Packet packet, byte[] packetData, int ipStart) {
		packet.setTtl(IPv4Util.decodeTTL(packetData, ipStart));
		packet.setSrc(IPv4Util.decodeSrc(packetData, ipStart));
		packet.setDst(IPv4Util.decodeDst(packetData, ipStart));
		packet.setIpId(IPv4Util.decodeId(packetData, ipStart));
	}

	private void buildInternetProtocolV6Packet(Packet packet, byte[] packetData, int ipStart) {
		packet.setTtl(IPv6Util.decodeTTL(packetData, ipStart));
		packet.setSrc(IPv6Util.decodeSrc(packetData, ipStart));
		packet.setDst(IPv6Util.decodeDst(packetData, ipStart));
		packet.setIpId(IPv6Util.decodeId(packetData, ipStart));
	}
	
	
	public byte[] reassemble(Packet packet,byte[] packetData, int ipStart){
		//reassemble IP fragments
		byte[] reassmbledPacketData = packetData;
		
		if (packet.isFragmented()) {
			
				nl.sidn.pcap.packet.Datagram datagram = packet.getDatagram();
				byte[] fragmentPacketData = Arrays.copyOfRange(packetData, ipStart + packet.getIpHeaderLen(), ipStart + packet.getTotalLength());
				nl.sidn.pcap.packet.DatagramPayload payload = new nl.sidn.pcap.packet.DatagramPayload(packet.getFragOffset(), fragmentPacketData);
				datagrams.put(datagram, payload);

				if (packet.isLastFragment()) {
					Collection<nl.sidn.pcap.packet.DatagramPayload> datagramPayloads = datagrams.removeAll(datagram);
					if (datagramPayloads != null && datagramPayloads.size() > 0) {
						reassmbledPacketData = Arrays.copyOfRange(packetData, 0, ipStart + packet.getIpHeaderLen()); // Start re-fragmented packet with IP header from current packet
				
						int reassembledFragments = 0;
						nl.sidn.pcap.packet.DatagramPayload prev = null;
						for (nl.sidn.pcap.packet.DatagramPayload datagramPayload : datagramPayloads) {
							if (prev == null && datagramPayload.getOffset() != 0) {
								if(LOG.isDebugEnabled()){
									LOG.debug("Datagram chain not starting at 0. Probably received packets out-of-order. Can't reassemble this packet.");
								}
								//do not even try to reasemble the data, probably corrupt packets.
								return new byte[0];
							}
							if (prev != null && !datagramPayload.linked(prev)) {
								if(LOG.isDebugEnabled()){
									LOG.debug("Broken datagram chain between " + datagramPayload + " and " + prev + ". Can't reassemble this packet.");
								}
								//do not even try to reasemble the data, probably corrupt packets.
								return new byte[0];
							}
							reassmbledPacketData = Bytes.concat(reassmbledPacketData, datagramPayload.getPayload());
							reassembledFragments++;
							prev = datagramPayload;
						}
						if (reassembledFragments == datagramPayloads.size()) {
							packet.setReassembledFragments(reassembledFragments);
						}
					}

				}
				else {
					//need last IP fragment before continu to tcp/udp reassembly
					reassmbledPacketData = new byte[0];
				}
		}
		return reassmbledPacketData;
	}

	public Multimap<nl.sidn.pcap.packet.Datagram, nl.sidn.pcap.packet.DatagramPayload> getDatagrams() {
		return datagrams;
	}

	public void setDatagrams(
			Multimap<nl.sidn.pcap.packet.Datagram, nl.sidn.pcap.packet.DatagramPayload> datagrams) {
		this.datagrams = datagrams;
	}
	
	
}

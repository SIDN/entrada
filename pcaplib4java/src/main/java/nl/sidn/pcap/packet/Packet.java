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

import nl.sidn.pcap.decoder.IPDecoder;

/**
 * Packet contains a combination of IP layer and UDP/TCP/DNS layer data
 * Fragmented IP is joined into a single Packet object
 * Fragmented UDP is joined into a single Packet object
 * TCP session with multiple DNS queries in a stream before the PSH or FIN will cause multiple
 * DNS messages to get added to the Packet object.
 * 
 */
public class Packet {

	public static final Packet NULL = new Packet();

	// network
	protected int len;
	protected long ts;
	protected long tsmicros;
	protected double tsUsec;
	// ip
	protected long ipId;
	protected int ttl;
	protected byte ipVersion;
	protected int ipHeaderLen;
	protected short protocol;
	protected String src;
	protected String dst;
	protected long fragOffset;
	protected boolean fragmented;
	protected boolean lastFragment;
	//ip fragments
	protected int reassembledFragments;
	//ipv6
	protected boolean fragmentFlagM;
	// udp tcp
	protected int reassembledTCPFragments;
	protected int srcPort;
	protected int dstPort;
	protected int tcpflow;
	// udp
	protected int udpsum;
	protected int udpLength;
	// tcp
	protected int tcpHeaderLen;
	protected long tcpSeq;
	protected long tcpAck;
	protected boolean tcpFlagNs;
	protected boolean tcpFlagCwr;
	protected boolean tcpFlagEce;
	protected boolean tcpFlagUrg;
	protected boolean tcpFlagAck;
	protected boolean tcpFlagPsh;
	protected boolean tcpFlagRst;
	protected boolean tcpFlagSyn;
	protected boolean tcpFlagFin;
	protected int tcpWindowSize;
	
	private int totalLength;
	protected int payloadLength;

	public TCPFlow getFlow() {
		return new TCPFlow(src, srcPort, dst, dstPort, protocol);
	}
	
	public Datagram getDatagram() {
		return new Datagram(getSrc(), getDst(), getIpId(), String.valueOf(getProtocol()), System.currentTimeMillis());
	}

	public long getIpId() {
		return ipId;
	}

	public void setIpId(long ipId) {
		this.ipId = ipId;
	}

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public long getTsmicros() {
		return tsmicros;
	}

	public void setTsmicros(long tsmicros) {
		this.tsmicros = tsmicros;
	}

	public double getTsUsec() {
		return tsUsec;
	}

	public void setTsUsec(double tsUsec) {
		this.tsUsec = tsUsec;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}

	public byte getIpVersion() {
		return ipVersion;
	}

	public void setIpVersion(byte ipVersion) {
		this.ipVersion = ipVersion;
	}

	public int getIpHeaderLen() {
		return ipHeaderLen;
	}

	public void setIpHeaderLen(int ipHeaderLen) {
		this.ipHeaderLen = ipHeaderLen;
	}

	public short getProtocol() {
		return protocol;
	}

	public void setProtocol(short protocol) {
		this.protocol = protocol;
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public String getDst() {
		return dst;
	}

	public void setDst(String dst) {
		this.dst = dst;
	}

	public int getSrcPort() {
		return srcPort;
	}

	public void setSrcPort(int srcPort) {
		this.srcPort = srcPort;
	}

	public int getDstPort() {
		return dstPort;
	}

	public void setDstPort(int dstPort) {
		this.dstPort = dstPort;
	}

	public int getUdpsum() {
		return udpsum;
	}

	public void setUdpsum(int udpsum) {
		this.udpsum = udpsum;
	}

	public int getUdpLength() {
		return udpLength;
	}

	public void setUdpLength(int udpLength) {
		this.udpLength = udpLength;
	}

	public int getTcpHeaderLen() {
		return tcpHeaderLen;
	}

	public void setTcpHeaderLen(int tcpHeaderLen) {
		this.tcpHeaderLen = tcpHeaderLen;
	}

	public long getTcpSeq() {
		return tcpSeq;
	}

	public void setTcpSeq(long tcpSeq) {
		this.tcpSeq = tcpSeq;
	}

	public long getTcpAck() {
		return tcpAck;
	}

	public void setTcpAck(long tcpAck) {
		this.tcpAck = tcpAck;
	}

	public boolean isTcpFlagNs() {
		return tcpFlagNs;
	}

	public void setTcpFlagNs(boolean tcpFlagNs) {
		this.tcpFlagNs = tcpFlagNs;
	}

	public boolean isTcpFlagCwr() {
		return tcpFlagCwr;
	}

	public void setTcpFlagCwr(boolean tcpFlagCwr) {
		this.tcpFlagCwr = tcpFlagCwr;
	}

	public boolean isTcpFlagEce() {
		return tcpFlagEce;
	}

	public void setTcpFlagEce(boolean tcpFlagEce) {
		this.tcpFlagEce = tcpFlagEce;
	}

	public boolean isTcpFlagUrg() {
		return tcpFlagUrg;
	}

	public void setTcpFlagUrg(boolean tcpFlagUrg) {
		this.tcpFlagUrg = tcpFlagUrg;
	}

	public boolean isTcpFlagAck() {
		return tcpFlagAck;
	}

	public void setTcpFlagAck(boolean tcpFlagAck) {
		this.tcpFlagAck = tcpFlagAck;
	}

	public boolean isTcpFlagPsh() {
		return tcpFlagPsh;
	}

	public void setTcpFlagPsh(boolean tcpFlagPsh) {
		this.tcpFlagPsh = tcpFlagPsh;
	}

	public boolean isTcpFlagRst() {
		return tcpFlagRst;
	}

	public void setTcpFlagRst(boolean tcpFlagRst) {
		this.tcpFlagRst = tcpFlagRst;
	}

	public boolean isTcpFlagSyn() {
		return tcpFlagSyn;
	}

	public void setTcpFlagSyn(boolean tcpFlagSyn) {
		this.tcpFlagSyn = tcpFlagSyn;
	}

	public boolean isTcpFlagFin() {
		return tcpFlagFin;
	}

	public void setTcpFlagFin(boolean tcpFlagFin) {
		this.tcpFlagFin = tcpFlagFin;
	}

	public int getReassembledFragments() {
		return reassembledFragments;
	}

	public void setReassembledFragments(int reassembledFragments) {
		this.reassembledFragments = reassembledFragments;
	}

	public int getTcpWindowSize() {
		return tcpWindowSize;
	}

	public void setTcpWindowSize(int tcpWindowSize) {
		this.tcpWindowSize = tcpWindowSize;
	}

	public int getTotalLength() {
		return totalLength;
	}

	public void setTotalLength(int totalLength) {
		this.totalLength = totalLength;
	}

	public long getFragOffset() {
		return fragOffset;
	}

	public void setFragOffset(long fragOffset) {
		this.fragOffset = fragOffset;
	}

	public int getPayloadLength() {
		return payloadLength;
	}

	public void setPayloadLength(int payloadLength) {
		this.payloadLength = payloadLength;
	}

	public boolean isIPv4() {
		return getIpVersion() == IPDecoder.IP_PROTOCOL_VERSION_4;
	}

	public boolean isIPv6() {
		return getIpVersion() == IPDecoder.IP_PROTOCOL_VERSION_6;
	}

	public boolean isFragmented() {
		return fragmented;
	}

	public void setFragmented(boolean fragmented) {
		this.fragmented = fragmented;
	}

	public boolean isLastFragment() {
		return lastFragment;
	}

	public void setLastFragment(boolean lastFragment) {
		this.lastFragment = lastFragment;
	}

	public boolean isFragmentFlagM() {
		return fragmentFlagM;
	}

	public void setFragmentFlagM(boolean fragmentFlagM) {
		this.fragmentFlagM = fragmentFlagM;
	}

	public int getReassembledTCPFragments() {
		return reassembledTCPFragments;
	}

	public void setReassembledTCPFragments(int reassembledTCPFragments) {
		this.reassembledTCPFragments = reassembledTCPFragments;
	}

	@Override
	public String toString() {
		return "Packet [len=" + len + ", ts=" + ts + ", tsmicros=" + tsmicros
				+ ", tsUsec=" + tsUsec + ", ipId=" + ipId + ", ttl=" + ttl
				+ ", ipVersion=" + ipVersion + ", ipHeaderLen=" + ipHeaderLen
				+ ", protocol=" + protocol + ", src=" + src + ", dst=" + dst
				+ ", fragOffset=" + fragOffset + ", fragmented=" + fragmented
				+ ", lastFragment=" + lastFragment + ", reassembledFragments="
				+ reassembledFragments + ", fragmentFlagM=" + fragmentFlagM
				+ ", srcPort=" + srcPort + ", dstPort=" + dstPort
				+ ", tcpflow=" + tcpflow + ", udpsum=" + udpsum
				+ ", udpLength=" + udpLength + ", tcpHeaderLen=" + tcpHeaderLen
				+ ", tcpSeq=" + tcpSeq + ", tcpAck=" + tcpAck + ", tcpFlagNs="
				+ tcpFlagNs + ", tcpFlagCwr=" + tcpFlagCwr + ", tcpFlagEce="
				+ tcpFlagEce + ", tcpFlagUrg=" + tcpFlagUrg + ", tcpFlagAck="
				+ tcpFlagAck + ", tcpFlagPsh=" + tcpFlagPsh + ", tcpFlagRst="
				+ tcpFlagRst + ", tcpFlagSyn=" + tcpFlagSyn + ", tcpFlagFin="
				+ tcpFlagFin + ", tcpWindowSize=" + tcpWindowSize
				+ ", totalLength=" + totalLength + ", payloadLength="
				+ payloadLength + "]";
	}
}

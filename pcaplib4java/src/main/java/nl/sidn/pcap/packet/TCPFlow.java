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

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

public class TCPFlow implements Comparable<TCPFlow> {
	private String src;
	private int srcPort;
	private String dst;
	private int dstPort;
	private short protocol;

	public TCPFlow() {

	}

	public TCPFlow(String src, int srcPort, String dst, int dstPort, short protocol) {
		this.src = src;
		this.srcPort = srcPort;
		this.dst = dst;
		this.dstPort = dstPort;
		this.protocol = protocol;
	}

	@Override
	public int compareTo(TCPFlow o) {
		return ComparisonChain.start()
				.compare(src, o.src)
				.compare(srcPort, o.srcPort)
				.compare(dst, o.dst)
				.compare(dstPort, o.dstPort)
				.compare(protocol, o.protocol)
				.result();
	}

	public String getSrc() {
		return src;
	}

	public void setSrc(String src) {
		this.src = src;
	}

	public int getSrcPort() {
		return srcPort;
	}

	public void setSrcPort(int srcPort) {
		this.srcPort = srcPort;
	}

	public String getDst() {
		return dst;
	}

	public void setDst(String dst) {
		this.dst = dst;
	}

	public int getDstPort() {
		return dstPort;
	}

	public void setDstPort(int dstPort) {
		this.dstPort = dstPort;
	}

	public short getProtocol() {
		return protocol;
	}

	public void setProtocol(short protocol) {
		this.protocol = protocol;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("src", src)
				.add("srcPort", srcPort).add("dst", dst)
				.add("dstPort", dstPort).add("protocol", protocol).toString();
	}
}
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
import com.google.common.collect.Ordering;

public class Datagram implements Comparable<Datagram> {
	private String src;
	private String dst;
	private Long id;
	private String protocol;
	private long time;

	public Datagram(String src, String dst, Long id, String protocol, long time) {
		this.src = src;
		this.dst = dst;
		this.id = id;
		this.protocol = protocol;
		this.time = time;
	}

	public long getTime() {
		return time;
	}

	@Override
	public int compareTo(Datagram o) {
		return ComparisonChain.start()
		                      .compare(src, o.src, Ordering.natural().nullsLast())
		                      .compare(dst, o.dst, Ordering.natural().nullsLast())
		                      .compare(id, o.id, Ordering.natural().nullsLast())
		                      .compare(protocol, o.protocol, Ordering.natural().nullsLast())
		                      .result();
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("src", src)
		                                              .add("dst", dst)
		                                              .add("id", id)
		                                              .add("protocol", protocol)
		                                              .toString();
	}
}
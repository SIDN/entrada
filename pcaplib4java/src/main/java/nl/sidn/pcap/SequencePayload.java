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
package nl.sidn.pcap;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * Class for re-assembly of TCP fragments
 * 
 */
public class SequencePayload implements Comparable<SequencePayload> {
	private Long seq;
	private byte[] payload;
	private long time;
	
	public SequencePayload(){}

	public SequencePayload(Long seq, byte[] payload,long time) {
		this.seq = seq;
		this.payload = payload;
		this.time = time;
	}

	@Override
	public int compareTo(SequencePayload o) {
		return ComparisonChain.start().compare(seq, o.seq)
		                              .compare(payload.length, o.payload.length)
		                              .result();
	}

	public boolean linked(SequencePayload o) {
		if ((seq + payload.length) == o.seq)
			return true;
		if ((o.seq + o.payload.length) == seq)
			return true;
		return false;
	}

	
	public long getTime() {
		return time;
	}

	public byte[] getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("seq", seq)
		                                              .add("len", payload.length)
		                                              .toString();
	}
}

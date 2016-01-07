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


public class RequestKey {

	private	int id;
	private	String qname;
	private	String src;
	private	int srcPort;
	private long time;
	
	public RequestKey() {}
	
	public RequestKey(int id, String qname, String src, int srcPort) {
		this(id, qname,src, srcPort, 0);
	}
	public RequestKey(int id, String qname, String src, int srcPort,long time) {
		this.id = id;
		this.qname = qname;
		this.src = src;
		this.srcPort = srcPort;
		this.time = time;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
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

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((qname == null) ? 0 : qname.hashCode());
		result = prime * result + ((src == null) ? 0 : src.hashCode());
		result = prime * result + srcPort;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RequestKey other = (RequestKey) obj;
		if (id != other.id)
			return false;
		if (qname == null) {
			if (other.qname != null)
				return false;
		} else if (!qname.equals(other.qname))
			return false;
		if (src == null) {
			if (other.src != null)
				return false;
		} else if (!src.equals(other.src))
			return false;
		if (srcPort != other.srcPort)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "RequestKey [id=" + id + ", qname=" + qname + ", src=" + src
				+ ", srcPort=" + srcPort + ", time=" + time + "]";
	}
	
}

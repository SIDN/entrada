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

public class ICMPPacket extends Packet{
	
	private short type;
	private short code;
	//contains IP hdr and (partial) dns response
	private Packet originalIPPacket;
	private boolean error;
	private boolean info;
	private int clientType;
	
	
	public short getType() {
		return type;
	}
	public void setType(short type) {
		this.type = type;
	}
	public short getCode() {
		return code;
	}
	public void setCode(short code) {
		this.code = code;
	}
	public Packet getOriginalIPPacket() {
		return originalIPPacket;
	}
	public void setOriginalIPPacket(Packet originalIPPacket) {
		this.originalIPPacket = originalIPPacket;
	}
	public boolean isError() {
		return error;
	}
	public void setError(boolean error) {
		this.error = error;
	}
	public boolean isInfo() {
		return info;
	}
	public void setInfo(boolean info) {
		this.info = info;
	}
	public int getClientType() {
		return clientType;
	}
	public void setClientType(int clientType) {
		this.clientType = clientType;
	}
	@Override
	public String toString() {
		return "ICMPPacket [type=" + type + ", code=" + code
				+ ", originalIPPacket=" + originalIPPacket + ", error=" + error
				+ ", info=" + info + ", clientType=" + clientType + "(" + super.toString() + ")]";
	}


}

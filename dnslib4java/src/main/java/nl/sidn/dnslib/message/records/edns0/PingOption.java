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
package nl.sidn.dnslib.message.records.edns0;

import java.util.Arrays;

import nl.sidn.dnslib.message.util.NetworkData;


/**
 * @see http://tools.ietf.org/html/draft-hubert-ulevitch-edns-ping-01
 * 
 *
 */
public class PingOption extends EDNS0Option{
	
	private byte[] ping;
	
	public PingOption(){}

	public PingOption(int code, int len, NetworkData opt) {
		super(code, len, opt);
	}

	public byte[] getPing() {
		return ping;
	}

	public void setPing(byte[] ping) {
		this.ping = ping;
	}

	@Override
	public String toString() {
		return "PingOption [ping=" + Arrays.toString(ping) + ", code=" + code
				+ ", len=" + len + "]";
	}

	@Override
	public void decode(NetworkData buffer) {
		byte[] ping = new byte[4];
		buffer.readBytes(ping);
		setPing(ping);
	}



}

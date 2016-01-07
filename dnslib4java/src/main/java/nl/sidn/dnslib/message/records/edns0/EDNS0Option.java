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

public class EDNS0Option {
	
	protected int  code;
	protected int len;
	
	public EDNS0Option(){}
	
	public EDNS0Option(int code, int len) {
		this.code = code;
		this.len = len;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
	}

	@Override
	public String toString() {
		return "EDNS0Option [code=" + code + ", len=" + len + "]";
	}
	
	

}

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


/**
 * http://tools.ietf.org/html/draft-vandergaast-edns-client-subnet-02
 *
 */
public class ClientSubnetOption extends EDNS0Option{

	private int fam;
	private int sourcenetmask;
	private int scopenetmask;
	private String address;
	
	public ClientSubnetOption(){}
	
	public ClientSubnetOption(int code, int len) {
		super(code, len);
	}

	public int getFam() {
		return fam;
	}

	public void setFam(int fam) {
		this.fam = fam;
	}

	public int getSourcenetmask() {
		return sourcenetmask;
	}

	public void setSourcenetmask(int sourcenetmask) {
		this.sourcenetmask = sourcenetmask;
	}

	public int getScopenetmask() {
		return scopenetmask;
	}

	public void setScopenetmask(int scopenetmask) {
		this.scopenetmask = scopenetmask;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	
	public boolean isIPv4(){
		return fam == 1;
	}
	
	public String export(){
		return (fam == 1? "4,": "6,") + address + "/" + sourcenetmask + "," + scopenetmask;
	}

	@Override
	public String toString() {
		return "ClientSubnetOption [fam=" + fam + ", sourcenetmask="
				+ sourcenetmask + ", scopenetmask=" + scopenetmask
				+ ", address=" + address + ", code=" + code + ", len=" + len
				+ "]";
	}

}

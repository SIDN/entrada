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

import nl.sidn.dnslib.message.util.NetworkData;

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
	
	public ClientSubnetOption(int code, int len,NetworkData buffer) {
		super(code, len, buffer);
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
	
	@Override
	public void decode(NetworkData buffer) {
		char fam = buffer.readUnsignedChar();
		short sourcenetmask = buffer.readUnsignedByte();
		short scopenetmask = buffer.readUnsignedByte();
		
		setFam(fam);
		setSourcenetmask(sourcenetmask);
		int addressOctets = (int) Math.ceil((double)sourcenetmask /8);
		setScopenetmask(scopenetmask);
		int addrLength = len - 4; //(-4 bytes for fam+sourse+scope mask)
		if(addrLength > 0){
			if(fam == 1){ //IP v4
				StringBuffer addressBuffer = new StringBuffer();
				for (int i = 0; i < addressOctets; i++) {
					if(addressBuffer.length() > 0){
						addressBuffer.append(".");
					}
					int addressPart = buffer.readUnsignedByte();
					addressBuffer.append(addressPart);
				}
				
				setAddress(addressBuffer.toString());
			}else if(fam == 2){ //v6
				StringBuilder sb = new StringBuilder();
				//read 2 byte blocks and convert to hex
				for (int i = 0; i < addressOctets; i=i+2) {
					if(sb.length() > 0){
						sb.append(":");
					}
					int addressPart = buffer.readUnsignedChar();
					sb.append(String.format("%04X", addressPart));
				}
				setAddress(sb.toString());
			}
		}
	}

}

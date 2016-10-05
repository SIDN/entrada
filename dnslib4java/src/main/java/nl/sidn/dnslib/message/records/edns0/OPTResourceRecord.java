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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import nl.sidn.dnslib.message.records.AbstractResourceRecord;
import nl.sidn.dnslib.message.util.DNSStringUtil;
import nl.sidn.dnslib.message.util.NetworkData;
import nl.sidn.dnslib.types.ResourceRecordType;

import org.apache.log4j.Logger;

/**
 * 
 * EDNS Header Flags (16 bits)

	Registration Procedures
	Standards Action
	Reference
	[RFC-ietf-dnsext-rfc2671bis-edns0-10]
	Bit 	Flag 	Description 	Reference 
	Bit 0	DO	DNSSEC answer OK	[RFC4035][RFC3225]
	Bit 1-15		Reserved	
 *
 */
public class OPTResourceRecord extends AbstractResourceRecord {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = Logger.getLogger(OPTResourceRecord.class);
	
	private static final char DNSSEC_DO_BIT_MASK = 0x8000; //1000 0000 0000 0000
	
	private static final int POWERDNS_EDNSPING_UDPSIZE = 1200;
	private static final int POWERDNS_EDNSPING_LENGTH = 4;
	
	private String name = ".";
	private ResourceRecordType type = ResourceRecordType.OPT;
	private char udpPlayloadSize = 4096;

	private char rdLeng = 0;
	
	private short rcode;
	private short version;
	private char flags;
	
	private boolean dnssecDo;
	
	private List<EDNS0Option> options = new ArrayList<>();
	
	public OPTResourceRecord(){}
	
	@Override
	public void decode(NetworkData buffer) {
		//name
		name = DNSStringUtil.readName(buffer);
		
		char type = buffer.readUnsignedChar();
		setType(ResourceRecordType.fromValue(type));
		
		udpPlayloadSize = buffer.readUnsignedChar();
		
		rcode = buffer.readUnsignedByte();
		
		version = buffer.readUnsignedByte();
		
		flags = buffer.readUnsignedChar();
		
		dnssecDo = (flags & DNSSEC_DO_BIT_MASK) == DNSSEC_DO_BIT_MASK;
		
		rdLeng = buffer.readUnsignedChar();
		if(rdLeng > 0){
			//not tested yet, add try catch just to be safe
			try {
				loadOptions(buffer);
			} catch (Exception e) {
				// ignore
				LOGGER.error("Could not decode OPT RR");
				e.printStackTrace();
			}
		} 
	}
	
	private void loadOptions(NetworkData buffer){
		if(rdLeng != buffer.bytesAvailable()){
			LOGGER.info("Incorrect edns rdata size");
			LOGGER.info("rdlength=" + (int)rdLeng + " and bytesavail:" + buffer.bytesAvailable());	
			return;
		}
				
		byte[] optionBytes = new byte[rdLeng];
		buffer.readBytes(optionBytes);
			
		NetworkData opt = new NetworkData(optionBytes);
		while(opt.isBytesAvailable()){
			options.add(decodeOption(opt));
		}

	}
	
	
	private EDNS0Option decodeOption(NetworkData opt){
	
		int optioncode = opt.readUnsignedChar();
		int optionlen = opt.readUnsignedChar();

		if(optioncode == 3 ){ //nsid
			NSidOption option = new NSidOption(optioncode, optionlen);
			if(optionlen > 0){
				//id present in packet
				byte[] iddata = new byte[optionlen];
				opt.readBytes(iddata);
				String id = new String(iddata);
				option.setId(id);
			}

			return option;
		}else if(optioncode == 5 ){
			//optioncode 5 can be two things:
			//1 dnssec DAU http://tools.ietf.org/html/rfc6975
			//2 ednsping http://tools.ietf.org/html/draft-hubert-ulevitch-edns-ping-01
				
			//powerdns edns ping uses udp size 1200
			//option length 4
			if(udpPlayloadSize == POWERDNS_EDNSPING_UDPSIZE &&
					optionlen == POWERDNS_EDNSPING_LENGTH ){
				PingOption pOption = new PingOption(optioncode, optionlen);
				
				byte[] ping = new byte[4];
				opt.readBytes(ping);
				pOption.setPing(ping);
				return pOption;
			}else{
				//must be dnssec DAU
				DNSSECOption option = new DNSSECOption(optioncode,optionlen);
				for (int i = 0; i <optionlen; i++) {
					int alg = opt.readUnsignedByte();
					option.addAlgorithm(alg);		
				}
				return option;
			}
		
		}else if(optioncode == 6 || optioncode == 7){
			//decode dnssec option
			DNSSECOption option = new DNSSECOption(optioncode,optionlen);
			for (int i = 0; i <optionlen; i++) {
				int alg = opt.readUnsignedByte();
				option.addAlgorithm(alg);		
			}
			return option;
		}else if(optioncode == 8){
			//decode clientsubnet option
			char fam = opt.readUnsignedChar();
			short sourcenetmask = opt.readUnsignedByte();
			short scopenetmask = opt.readUnsignedByte();
			
			ClientSubnetOption option = new ClientSubnetOption(optioncode, optionlen);
			option.setFam(fam);
			option.setSourcenetmask(sourcenetmask);
			int addressOctets = (int) Math.ceil((double)sourcenetmask /8);
			option.setScopenetmask(scopenetmask);
			int addrLength = optionlen - 4; //(-4 bytes for fam+sourse+scope mask)
			if(addrLength > 0){
				if(fam == 1){ //IP v4
					StringBuffer addressBuffer = new StringBuffer();
					for (int i = 0; i < addressOctets; i++) {
						if(addressBuffer.length() > 0){
							addressBuffer.append(".");
						}
						int addressPart = opt.readUnsignedByte();
						addressBuffer.append(addressPart);
					}
					
					option.setAddress(addressBuffer.toString());
				}else if(fam == 2){ //v6
					StringBuilder sb = new StringBuilder();
					//read 2 byte blocks and convert to hex
					for (int i = 0; i < addressOctets; i=i+2) {
						if(sb.length() > 0){
							sb.append(":");
						}
						int addressPart = opt.readUnsignedChar();
						sb.append(String.format("%04X", addressPart));
					}
					option.setAddress(sb.toString());
				}
			}
			return option;
		}else{
			//catch all, for experimental edns options
			//read data, but ignore values
			byte[] data = new byte[optionlen];
			opt.readBytes(data);
			EDNS0Option ednsOption = new EDNS0Option(optioncode, optionlen);
			return ednsOption;
		}
	}
	
	public static String convertAddress(byte[] data) {
		//byte[] addr = new byte[16];
		//System.arraycopy(data, 0, addr, 0, addr.length);
		try {
			return InetAddress.getByAddress(data).getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void encode(NetworkData buffer) {
		
		LOGGER.debug("encode");
		
		//write the name 
		buffer.writeByte(0);
		
		//write the opt type
		buffer.writeChar(type.getValue());
		
		//write the supported udp size
		buffer.writeChar(udpPlayloadSize);
	
		//write extended rcode
		buffer.writeByte(0x0); 
		
		//write version
		buffer.writeByte(0x0); 
		
		//default all flags off
		char flags = 0x0;
		
		//dnssec enabled, signal with do bit is on
		flags = (char)(flags | DNSSEC_DO_BIT_MASK);
		
		//write all the flags
		buffer.writeChar(flags); 

		//write the length of the rdata section
		buffer.writeChar(rdLeng); 
	}


	@Override
	public String toString() {
		return "OPTResourceRecord [name=" + name + ", type=" + type
				+ ", udpPlayloadSize=" + (int)udpPlayloadSize + ", rdLeng=" + (int)rdLeng
				+ ", doBit=" + dnssecDo + "]";
	}
	
	@Override
	public JsonObject toJSon(){
		JsonObjectBuilder builder = Json.createObjectBuilder();
		return builder.
			add("name", name).
			add("type", type.name()).
			add("payload-size", (int)udpPlayloadSize).
			add("rcode", rcode).
			add("flags", (int)flags).
			add("rdata", Json.createObjectBuilder().
				add("do", dnssecDo)).
			build();
	}
	
	public boolean getDnssecDo() {
		return dnssecDo;
	}
	
	public void setDnssecDo(boolean dnssecDo) {
		this.dnssecDo = dnssecDo;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public char getUdpPlayloadSize() {
		return udpPlayloadSize;
	}

	public void setUdpPlayloadSize(char udpPlayloadSize) {
		this.udpPlayloadSize = udpPlayloadSize;
	}

	public short getRcode() {
		return rcode;
	}

	public void setRcode(short rcode) {
		this.rcode = rcode;
	}

	public short getVersion() {
		return version;
	}

	public void setVersion(short version) {
		this.version = version;
	}

	public char getFlags() {
		return flags;
	}

	public void setFlags(char flags) {
		this.flags = flags;
	}

	@Override
	public String toZone(int maxLength) {
		return "";
	}

	public List<EDNS0Option> getOptions() {
		return options;
	}

	public void setOptions(List<EDNS0Option> options) {
		this.options = options;
	}

}

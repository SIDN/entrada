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
package nl.sidnlabs.dnslib.types;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Reference
	[RFC-ietf-dnsext-rfc6195bis-05]
	Note
	As noted in [RFC-cheshire-dnsext-multicastdns-15], Multicast DNS can only
	carry DNS records with classes in the range 0-32767. Classes in the range 32768 to
	65535 are incompatible with Multicast DNS.
  Decimal 	Hexadecimal 	Name 	Reference 
	0	0x0000	Reserved	[RFC-ietf-dnsext-rfc6195bis-05]
	1	0x0001	Internet (IN)	[RFC1035]
	2	0x0002	Unassigned	
	3	0x0003	Chaos (CH)	[D. Moon, "Chaosnet", A.I. Memo 628, Massachusetts Institute of Technology Artificial Intelligence Laboratory, June 1981.]
	4	0x0004	Hesiod (HS)	[Dyer, S., and F. Hsu, "Hesiod", Project Athena Technical Plan - Name Service, April 1987.]
	5-253	0x0005-0x00FD	Unassigned	
	254	0x00FD	QCLASS NONE	[RFC2136]
	255	0x00FF	QCLASS * (ANY)	[RFC1035]
	256-65279	0x0100-0xFEFF	Unassigned	
	65280-65534	0xFF00-0xFFFE	Reserved for Private Use	[RFC-ietf-dnsext-rfc6195bis-05]
	65535	0xFFFF	Reserved	[RFC-ietf-dnsext-rfc6195bis-05]

 *
 */
public enum ResourceRecordClass {

	RESERVED(0),
	IN(1),
	//2 UNASSIGNED
	CH(3),
	HS(4),
	//5-253 Unassigned
	NONE(254),
	ANY(255),
	//256-65279 Unassigned
	//65280-65534 private
	//65535 RESERVED
	UNASSIGNED(-1),
	PRIVATE(-2),
	UNKNOWN(-3);
	
	private int value;
	
	private static Map<String, ResourceRecordClass> classes = new HashMap<>();
	private static Map<Integer, ResourceRecordClass> classesToInt = new HashMap<>();
	
	static{
		ResourceRecordClass[] values = values();
		for (ResourceRecordClass classz : values) {
			classes.put(classz.name(), classz);
			classesToInt.put(new Integer(classz.getValue()), classz);
		}
	}
	
	private ResourceRecordClass(int value){
		this.value = value;
	}

	public int getValue() {
		return value;
	}
	
	public static ResourceRecordClass fromString(String name){
		return classes.get(StringUtils.upperCase(name));
	}
	
	public static ResourceRecordClass fromValue(int value){
		ResourceRecordClass classz = classesToInt.get(new Integer(value));
		
		if(classz == null){
			//2 UNASSIGNED
			//5-253 Unassigned
			//256-65279 Unassigned
			if( (value == 2) ||
				(value >= 5 &&  value <= 253) ||
				(value >= 256 &&  value <= 65279) 
			  ){
				return UNASSIGNED;
			}else if(value >= 65280 &&  value <= 65534){
				//65280-65534 private
				return PRIVATE;
			}
			//no match
			return UNKNOWN;
		}
		
		return classz;
	}
}

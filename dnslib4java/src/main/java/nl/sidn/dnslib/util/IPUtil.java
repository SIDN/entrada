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
package nl.sidn.dnslib.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.net.InetAddresses;

public class IPUtil {

	public static String reverseIpv6(String ipv6){
		List<String> ipv6Parts = new ArrayList<>();
		String[] parts = StringUtils.split(ipv6, ":");
		int len = parts.length;
		for (int i = len-1; i >= 0; i--) {
			String part = parts[i];
			
			for (int j = 3; j >= 0; j--) {
				
				String octet = part.substring(j, j+1);
				ipv6Parts.add((String)octet);
			}
		}
		
		return StringUtils.join(ipv6Parts, ".") + ".ip6.arpa.";
	}
	
	public static String reverseIpv4(String ipv4){
		List<String> ipv4Parts = new ArrayList<>();
		String[] parts = StringUtils.split(ipv4, ".");
		int len = parts.length;
		for (int i = len-1; i >= 0; i--) {
			String part = parts[i];
			
			ipv4Parts.add(part);
		}
		
		return StringUtils.join(ipv4Parts, ".") + ".in-addr.arpa.";
	}
	
	public static String expandIPv6(String ip){
		String[] sections = StringUtils.splitByWholeSeparatorPreserveAllTokens(ip, ":");
		StringBuilder sb = new StringBuilder();
		for (String section : sections) {
			if(section.length() == 0){
				int missing = (8 - sections.length)+1;
				for (int i = 0; i < missing; i++) {
					sb.append("0000:");
				}
			}else if(section.length() < 4){
				String paddedSection = StringUtils.leftPad(section, 4, "0");
				sb.append(paddedSection + ":");
			}else{
				sb.append(section + ":");
			}
		}
		String expanded = sb.toString();
		return StringUtils.removeEnd(expanded, ":");
	}
	
	public static byte[] ipv4tobytes(String ip){
		String[] parts = StringUtils.split(ip, ".");
		byte[] bytes = new byte[4];
		for (int i = 0; i < parts.length ; i++) {
			bytes[i] =  (byte)Integer.parseInt(parts[i]);
		}

		return bytes;
	}
	
	public static byte[] ipv6tobytes(String ip){
		int parts = StringUtils.countMatches(ip, ":");
		int missing = 7 - parts;
		
		for (int i = 0; i < missing ; i++) {
			ip = ip + ":0000";
		}

		return InetAddresses.forString(ip).getAddress();
	}

}

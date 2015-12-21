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

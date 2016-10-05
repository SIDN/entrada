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

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class NameUtil {
	
	private static Pattern FQDN;
	private static final String DOMAIN_NAME_PATTERN = "^((?!-)[A-Za-z0-9-_]{1,63}(?<!-)\\.)*([A-Za-z]{2,2})*(\\.)*$";
	
	static {
		FQDN = Pattern.compile(DOMAIN_NAME_PATTERN);
	}
 

	public static String reverse(String name) {
		String[] parts = StringUtils.split(name, '.');
		if (parts.length > 0) {
			String reverse = "";
			for (int i = parts.length - 1; i >= 0; i--) {
				reverse = reverse + "." + parts[i];
			}
			return reverse;
		}
		return null;
	}
	
	public static Domaininfo getDomain(String name){
		if(name == null || name.length() == 0){
			return new Domaininfo(null,0);
		}
		if(StringUtils.equals(name, ".")){
			return new Domaininfo(name,0);
		}
		
		String[] parts = StringUtils.split(name, ".");

		if(parts != null && parts.length > 0){
			if( parts.length == 1){
				//only 1 label present
				return new Domaininfo(parts[0],1);
			}
			//all others have more than 1 label, get last 2
			return new Domaininfo(parts[parts.length-2] + "." + parts[parts.length-1],parts.length);
		}
		
		return new Domaininfo(name,0);
		
	}
	
	public static boolean isFqdn(String domain){
		return FQDN.matcher(domain).find();
	}
	
	public static String getSecondLevel(String name) {
		if(name == null || name.length() == 0){
			return null;
		}
		
		String[] parts = StringUtils.split(name, '.');
		if (parts.length == 0) {
			return name;
		}else if (parts.length == 1) {
			 return parts[parts.length-1];
		}else {
			return parts[parts.length-2]  + "." + parts[parts.length-1];
		}
		
	}
	


}

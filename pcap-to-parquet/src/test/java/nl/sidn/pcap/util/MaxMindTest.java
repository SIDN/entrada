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
package nl.sidn.pcap.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import nl.sidn.dnslib.util.IPUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MaxMindTest {

	private static GeoLookupUtil geo;
	
	@Before
	public void setup(){
		ClassLoader classLoader = getClass().getClassLoader();
		Settings.setPath(classLoader.getResource("test-settings.properties").getFile() );
		Settings.getInstance().setSetting(Settings.STATE_LOCATION, "/tmp/");
		geo = new GeoLookupUtil();
	}
	
	@Test
	public void sidnIPASN()throws UnknownHostException{
		InetAddress addr = InetAddress.getByAddress(IPUtil.ipv4tobytes("94.198.159"));
		String asn = geo.lookupASN(addr, true);
		Assert.assertEquals("AS1140", asn);
	}
	
	@Test
	public void sidnIPNetmaskASN()throws UnknownHostException{
		InetAddress addr = InetAddress.getByAddress(IPUtil.ipv4tobytes("94.198.159"));
		String country = geo.lookupCountry(addr);
		Assert.assertEquals("NL", country);
		
		
		addr = InetAddress.getByAddress(IPUtil.ipv6tobytes("2a00:d78::147:94:198:152"));
		country = geo.lookupCountry(addr);
		Assert.assertEquals("NL", country);
	}
	
	
	

}

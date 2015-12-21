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

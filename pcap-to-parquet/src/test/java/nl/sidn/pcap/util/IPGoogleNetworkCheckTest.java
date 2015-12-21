package nl.sidn.pcap.util;

import nl.sidn.pcap.ip.GoogleResolverCheck;
import nl.sidn.pcap.ip.OpenDNSResolverCheck;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IPGoogleNetworkCheckTest {

	@Before
	public void setup(){
		ClassLoader classLoader = getClass().getClassLoader();
		Settings.setPath(classLoader.getResource("test-settings.properties").getFile() );
	}
	
	@Test
	public void createGoogleCheckTest(){
		GoogleResolverCheck check = new GoogleResolverCheck();
		check.update();
		Assert.assertTrue(check.getSize() > 0);
	}
	
	@Test
	public void createOpenDNSCheckTest(){
		OpenDNSResolverCheck check = new OpenDNSResolverCheck();
		check.update();
		Assert.assertTrue(check.getSize() > 0);
	}
}

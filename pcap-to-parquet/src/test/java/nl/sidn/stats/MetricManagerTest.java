package nl.sidn.stats;

import nl.sidn.pcap.util.Settings;

import org.junit.Before;
import org.junit.Test;

public class MetricManagerTest {
	
	
	@Before
	public void setup(){
		ClassLoader classLoader = getClass().getClassLoader();
		Settings.setPath(classLoader.getResource("test-settings.properties").getFile() );
	}
	
	@Test
	public void testSendMsg(){
		MetricManager.getInstance().send(Settings.getInstance().getSetting("graphite.prefix") + ".dns.count", 250);
	}

}

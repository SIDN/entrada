package nl.sidn.pcap;

import org.junit.Test;

public class MainTest {
	
	@Test
	public void testRun(){
		Main main = new Main();

		ClassLoader classLoader = getClass().getClassLoader();
		String[] args = {"ns1.dns.nl",classLoader.getResource("test-settings.properties").getFile(),
				"/tmp/","/tmp", "/tmp"};
		main.run(args);
	}
	
}

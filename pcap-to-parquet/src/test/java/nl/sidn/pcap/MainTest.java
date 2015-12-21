package nl.sidn.pcap;

import org.junit.Test;

public class MainTest {
	
	@Test
	public void testRun(){
		Main main = new Main();

		ClassLoader classLoader = getClass().getClassLoader();
		String[] args = {"ns1.dns.nl",classLoader.getResource("test-settings.properties").getFile(),
				"/Users/maarten/sidn/development/tmp/pcap/input","/Users/maarten/sidn/development/tmp/pcap/parquet", "/tmp"};
		main.run(args);
	}
	
}

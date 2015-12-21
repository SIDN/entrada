package nl.sidn.pcap.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SettingTest {

	@Before
	public void setup(){
		ClassLoader classLoader = getClass().getClassLoader();
		Settings.setPath(classLoader.getResource("test-settings.properties").getFile() );
	}
	@Test
	public void readSettings(){
		String val = Settings.getInstance().getSetting(Settings.RESOLVER_LIST_GOOGLE);
		Assert.assertEquals("https://developers.google.com/speed/public-dns/faq", val);
	}
}

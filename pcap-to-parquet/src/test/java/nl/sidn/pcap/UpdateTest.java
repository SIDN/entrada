package nl.sidn.pcap;

import org.junit.Test;

public class UpdateTest {
	

	@Test
	public void updateTest(){
		Update update = new Update();
		
		ClassLoader classLoader = getClass().getClassLoader();
		String[] args = {classLoader.getResource("test-settings.properties").getFile(), "/tmp" };
		update.update(args);
	}

}

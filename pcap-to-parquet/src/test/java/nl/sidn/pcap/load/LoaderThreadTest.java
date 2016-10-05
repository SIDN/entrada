package nl.sidn.pcap.load;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import nl.sidn.pcap.support.PacketCombination;
import nl.sidn.pcap.util.Settings;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LoaderThreadTest {
	
	@Before
	public void before(){
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("test-settings.properties").getFile());
		System.out.println(file.getAbsolutePath());
		
		Settings.setPath(file.getAbsolutePath());
		Settings.getInstance().forServer("test-server");
	}
	
	@Test
	public void loadValidPcap(){
		BlockingQueue<PacketCombination> queue = new LinkedBlockingQueue<PacketCombination>(); 
		LoaderThread lt = new LoaderThread(queue, "/tmp/","/tmp/","/tmp/");
		
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("entrada-test-pcap-3-queries.pcap.gz").getFile());
		lt.read(file.getAbsolutePath());
		
		Assert.assertEquals(3, queue.size());
	}

}

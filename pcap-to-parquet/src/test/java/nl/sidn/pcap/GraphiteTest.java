package nl.sidn.pcap;

import nl.sidn.pcap.util.Settings;
import nl.sidn.stats.GraphiteAdapter;
import nl.sidn.stats.Metric;

import org.junit.Before;
import org.junit.Test;

/**
 * These tests need a working graphite configuration
 *
 */
public class GraphiteTest {
	
	@Before
	public void setup(){
		ClassLoader classLoader = getClass().getClassLoader();
		Settings.setPath(classLoader.getResource("test-settings.properties").getFile() );
		Settings.getInstance().setName("test");
	}
	
	@Test
	public void sendMetric(){
		GraphiteAdapter ga = new GraphiteAdapter();
		ga.connect();
		Metric m = new Metric(Settings.getInstance().getSetting("graphite.prefix") + ".dev.graphitetest.count", 100, (System.currentTimeMillis()/1000));
		ga.send(m.toString());
		ga.close();
		
	}
	
	@Test
	public void sendMultiMetric(){
		StringBuffer buffer = new StringBuffer();
		String prefix = Settings.getInstance().getSetting("graphite.prefix");
		for (int i = 0; i < 10; i++) {
			Metric m = new Metric(prefix  + ".dev.graphitetest.count", 100+i, (System.currentTimeMillis()/1000)+(i*10));
			buffer.append(m.toString() + "\n");
		}
		
		GraphiteAdapter ga = new GraphiteAdapter();
		ga.connect();
		ga.send(buffer.toString());
		ga.close();
		
	}

}

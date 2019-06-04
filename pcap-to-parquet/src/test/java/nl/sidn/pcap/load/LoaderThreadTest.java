package nl.sidn.pcap.load;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import nl.sidn.pcap.config.Settings;
import nl.sidn.pcap.support.PacketCombination;

public class LoaderThreadTest {

  @Before
  public void before() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("test-settings.properties").getFile());
    System.out.println(file.getAbsolutePath());

    Settings.setPath(file.getAbsolutePath());
    Settings.getInstance().forServer("test-server");
  }

  @Test
  public void loadValidPcap() {
    BlockingQueue<PacketCombination> queue = new LinkedBlockingQueue<PacketCombination>();
    PcapProcessor lt = new PcapProcessor(queue, "/tmp/", "/tmp/", "/tmp/");

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader
        .getResource("pcap/ns1.example.nl/ns1.example.nl_2-txt-queries.pcap.gz").getFile());
    lt.read(file.getAbsolutePath());

    Assert.assertEquals(5, queue.size());
  }

}

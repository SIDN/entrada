package nl.sidn.metric;

import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import nl.sidn.pcap.Application;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class MetricManagerTest {

  @Autowired
  private MetricManager metricManager;

  @Test
  public void testMetricOk() {
    assertNotNull(metricManager);
    // metricManager.send("test", 0);
  }



}

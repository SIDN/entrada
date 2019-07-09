package nl.sidnlabs.entrada.parquet;

import static org.junit.Assert.assertNotNull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.metric.SimpleMetric;

public class KryoTest {

  @Test
  public void testMetricMapOk() {
    Kryo kryo = new Kryo();

    ServerContext ctx = new ServerContext();
    ctx.setServer("ns1.dns.nl");
    HistoricalMetricManager mgr = new HistoricalMetricManager(ctx);
    mgr.record("my.metric", 1, new Date().getTime());

    // serialize
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(10 * 1024);
    Output out = new Output(byteArrayOutputStream);
    kryo.writeClassAndObject(out, mgr.getMetricCache());

    // deserialize
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(out.toBytes());
    Input input = new Input(byteArrayInputStream);
    Map<String, List<SimpleMetric>> obj = (Map<String, List<SimpleMetric>>) kryo.readClassAndObject(input);

    assertNotNull(obj);

  }

}

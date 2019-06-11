package nl.sidnlabs.entrada.parquet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.springframework.security.web.util.matcher.IpAddressMatcher;

public class SubnetUtileTest {

  @Test
  public void test() {
    IpAddressMatcher m = new IpAddressMatcher("74.125.18.0/25");

    assertTrue(m.matches("74.125.18.1"));

    assertFalse(m.matches("10.125.18.1"));
  }

}

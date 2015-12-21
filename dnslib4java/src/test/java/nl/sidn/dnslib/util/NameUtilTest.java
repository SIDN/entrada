package nl.sidn.dnslib.util;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class NameUtilTest {

	@Test
	public void get2ndLevel() {
		Assert.assertEquals("sidn.nl",
				NameUtil.getDomain("test.www.sidn.nl.").name);
		Assert.assertEquals("sidn.nl", NameUtil.getDomain("www.sidn.nl.").name);
		Assert.assertEquals("sidn.nl", NameUtil.getDomain(".sidn.nl.").name);
		Assert.assertEquals("sidn.nl", NameUtil.getDomain("sidn.nl.").name);
		Assert.assertEquals("nl", NameUtil.getDomain("nl.").name);
		Assert.assertEquals("nl", NameUtil.getDomain(".nl.").name);
		Assert.assertEquals("nl", NameUtil.getDomain("nl").name);
		Assert.assertEquals(".", NameUtil.getDomain(".").name);
		Assert.assertEquals(null, NameUtil.getDomain("").name);
		Assert.assertEquals(null, NameUtil.getDomain(null).name);

	}

	@Test
	public void get2ndLevelFromEmail() {
		// email address
		Assert.assertEquals("wullink@sidn.nl",
				NameUtil.getDomain("maarten.wullink@sidn.nl.").name);
		System.out.println("maarten.wullink@sidn.nl. = "
				+ NameUtil.getDomain("maarten.wullink@sidn.nl."));
	}

	private static final String DOMAIN_NAME_PATTERN = "^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)*([A-Za-z]{2,2})*(\\.)*$";

	@Test
	public void split() {
		String[] parts = StringUtils.split("test.nl.", ".");
		System.out.println("2nd: " + NameUtil.getDomain("www.sidn.nl."));
		System.out.println("2nd: " + NameUtil.getDomain(".sidn.nl."));
		System.out.println("2nd: " + NameUtil.getDomain("2.www.sidn.nl."));
		System.out.println("2nd: " + NameUtil.getDomain(".nl."));
		System.out.println("2nd: " + NameUtil.getDomain("."));
		System.out.println("2nd: " + NameUtil.getDomain(null));
		System.out.println("2nd: " + NameUtil.getDomain("test .sidn.nl."));
	}
}

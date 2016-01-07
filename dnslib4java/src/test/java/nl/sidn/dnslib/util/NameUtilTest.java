/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].
 *
 */	
package nl.sidn.dnslib.util;

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

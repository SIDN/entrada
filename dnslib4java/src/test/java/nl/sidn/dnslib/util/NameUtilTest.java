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

import org.junit.Assert;
import org.junit.Test;

public class NameUtilTest {

	@Test
	public void get2ndLevel() {
		Assert.assertEquals("sidn.nl",NameUtil.getDomain("test.www.sidn.nl.").name);
		Assert.assertEquals("sidn.nl", NameUtil.getDomain("www.sidn.nl.").name);
		Assert.assertEquals("sidn.nl", NameUtil.getDomain(".sidn.nl.").name);
		Assert.assertEquals("sidn.nl", NameUtil.getDomain("sidn.nl.").name);
		Assert.assertEquals("nl", NameUtil.getDomain("nl.").name);
		Assert.assertEquals("nl", NameUtil.getDomain(".nl.").name);
		Assert.assertEquals("nl", NameUtil.getDomain("nl").name);
		Assert.assertEquals(".", NameUtil.getDomain(".").name);
		Assert.assertEquals(null, NameUtil.getDomain("").name);
		Assert.assertEquals(null, NameUtil.getDomain(null).name);
		Assert.assertEquals("sidn.nl", NameUtil.getDomain("test .sidn.nl.").name);
	}

	@Test
	public void get2ndLevelFromEmail() {
		// email address
		Assert.assertEquals("wullink@sidn.nl",NameUtil.getDomain("maarten.wullink@sidn.nl.").name);
	}

}

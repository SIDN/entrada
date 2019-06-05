/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidnlabs.dnslib.util;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class NameUtilTest {

  @Test
  public void tldIs2ndLevelTest() {
    Assert.assertEquals("sidn.nl", NameUtil.getDomain("test.www.sidn.nl.").getName());
    Assert.assertEquals("sidn.nl", NameUtil.getDomain("www.sidn.nl.").getName());
    Assert.assertEquals("sidn.nl", NameUtil.getDomain(".sidn.nl.").getName());
    Assert.assertEquals("sidn.nl", NameUtil.getDomain("sidn.nl.").getName());
    Assert.assertEquals("nl", NameUtil.getDomain("nl.").getName());
    Assert.assertEquals("nl", NameUtil.getDomain(".nl.").getName());
    Assert.assertEquals("nl", NameUtil.getDomain("nl").getName());
    Assert.assertEquals(".", NameUtil.getDomain(".").getName());
    Assert.assertEquals(null, NameUtil.getDomain("").getName());
    Assert.assertEquals(null, NameUtil.getDomain(null).getName());
    Assert.assertEquals("sidn.nl", NameUtil.getDomain("test .sidn.nl.").getName());
  }

  @Test
  public void tldIs3rdLevelTest() {
    List<DomainParent> parents = new ArrayList<>();
    parents.add(new DomainParent(".sidn.test.nl.", ".sidn.test.nl", 3));

    Assert.assertEquals("name.sidn.test.nl",
        NameUtil.getDomain("www.name.sidn.test.nl.", parents).getName());
    Assert.assertEquals("nu.nl", NameUtil.getDomain("www.nu.nl.", parents).getName());
    Assert.assertEquals("datumprikker.nl",
        NameUtil.getDomain("datumprikker.nl.", parents).getName());
    Assert.assertEquals("test.net", NameUtil.getDomain("www.test.net.", parents).getName());
    Assert.assertEquals("stroebarg.nl", NameUtil.getDomain("stroebarg.nl.", parents).getName());
  }

  @Test
  public void emailAddress2ndLevelTest() {
    // email address should not happen, but sometimes we do see these in the qname.
    Assert.assertEquals("test@example.com",
        NameUtil.getDomain("email.test@example.com.").getName());
  }

  @Test
  public void domainWith5thLevelAndTldSuffix() {
    // get the parent
    DomainParent dp = new DomainParent(".test.co.example.", ".test.co.example", 3);
    List<DomainParent> dps = new ArrayList<>();
    dps.add(dp);

    // test fqdn (including final dot)
    Domaininfo info = NameUtil.getDomainWithSuffixList("name.foo.test.co.example.", dps);

    Assert.assertNotNull(info);
    Assert.assertNotNull(info.getName());
    Assert.assertEquals("foo.test.co.example", info.getName());
    Assert.assertTrue(info.getLabels() == 5);

  }

  @Test
  public void domainWith2ndLevelAndTldSuffix() {
    // get the parent
    DomainParent dp = new DomainParent(".example.", ".example", 1);
    List<DomainParent> dps = new ArrayList<>();
    dps.add(dp);

    // test fqdn (including final dot)
    Domaininfo info = NameUtil.getDomainWithSuffixList("name.example.", dps);

    Assert.assertNotNull(info);
    Assert.assertNotNull(info.getName());
    Assert.assertEquals("name.example", info.getName());
    Assert.assertTrue(info.getLabels() == 2);

  }
}

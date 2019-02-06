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
package nl.sidn.pcap.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.google.common.net.InetAddresses;

public class GeoLookupUtilTest {

  private static GeoLookupUtil geo;

  @Before
  public void setup() {
    ClassLoader classLoader = getClass().getClassLoader();
    Settings.setPath(classLoader.getResource("test-settings.properties").getFile());
    Settings.getInstance().setSetting(Settings.STATE_LOCATION,
        "/Users/maartenwullink/sidn/development/tmp/entrada/");
    geo = new GeoLookupUtil();
  }

  @Test
  public void testAsnLookupIpv4() throws UnknownHostException {
    InetAddress addr = InetAddresses.forString("94.198.159.1");
    String asn = geo.lookupASN(addr);
    Assert.assertEquals("1140", asn);
  }

  @Test
  public void testAsnLookupIpv6() throws UnknownHostException {
    InetAddress addr = InetAddresses.forString("2a00:d78::147:94:198:152");
    String asn = geo.lookupASN(addr);
    Assert.assertEquals("1140", asn);
  }

  @Test
  public void testGeoLookupIpv4() throws UnknownHostException {
    InetAddress addr = InetAddresses.forString("94.198.159.1");
    String country = geo.lookupCountry(addr);
    Assert.assertEquals("NL", country);
  }

  @Test
  public void testGeoLookupIpv6() throws UnknownHostException {
    InetAddress addr = InetAddresses.forString("2a00:d78::147:94:198:152");
    String country = geo.lookupCountry(addr);
    Assert.assertEquals("NL", country);
  }



}

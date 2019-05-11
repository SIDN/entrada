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

import com.google.common.collect.Lists;
import com.google.common.net.InetAddresses;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;

public class GeoLookupUtilTest {

  private static GeoLookupUtil geo;

  @Before
  public void setup() {
    String userHome = System.getProperty("user.home");
    String path = userHome + "/sidn/development/tmp/entrada/";
    // make sure you have a maxmind folder in $HOME/sidn/development/tmp/entrada/ with the correct .mmdb files
    System.out.println("path = " + path);
    ClassLoader classLoader = getClass().getClassLoader();
    Settings.setPath(classLoader.getResource("test-settings.properties").getFile());
    Settings.getInstance().setSetting(Settings.STATE_LOCATION, path);
    geo = new GeoLookupUtil();
  }

  @Test
  public void testAsnLookupIpv4() {
    InetAddress addr = InetAddresses.forString("94.198.159.1");
    String asn = geo.lookupASN(addr);
    Assert.assertEquals("1140", asn);
  }

  @Test
  public void testAsnLookupIpv6() {
    InetAddress addr = InetAddresses.forString("2a00:d78::147:94:198:152");
    String asn = geo.lookupASN(addr);
    Assert.assertEquals("1140", asn);
  }

  @Test
  public void testGeoLookupIpv4() {
    InetAddress addr = InetAddresses.forString("94.198.159.1");
    String country = geo.lookupCountry(addr);
    Assert.assertEquals("NL", country);

    byte[] addrByte = addr.getAddress();
    country = geo.lookupCountry(addr);
    Assert.assertEquals("NL", country);
  }

  @Test
  public void testGeoLookupIpv6() {
    InetAddress addr = InetAddresses.forString("2a00:d78::147:94:198:152");
    String country = geo.lookupCountry(addr);
    Assert.assertEquals("NL", country);
  }

  @Test
  public void lookup() {
    String ip = "81.164.126.240";
    assertEquals("BE", geo.lookupCountry(ip));
    assertEquals("6848", geo.lookupASN(ip));

    ip = "8.8.8.8";
    assertEquals("US", geo.lookupCountry(ip));
    assertEquals("15169", geo.lookupASN(ip));

    ip = "212.114.98.233";
    assertEquals("NL", geo.lookupCountry(ip));
    assertEquals("12859", geo.lookupASN(ip));

    ip = "74.80.115.0";
    assertEquals("CN", geo.lookupCountry(ip));
    assertEquals("715", geo.lookupASN(ip));

    ip = "74.80.89.0";
    assertEquals("DE", geo.lookupCountry(ip));
    assertEquals("715", geo.lookupASN(ip));

    ip = "2620:0171:00F7:0000::";
    assertEquals("ZA", geo.lookupCountry(ip));
    assertEquals("42", geo.lookupASN(ip));

    ip = "2001:0500:0015:0600::";
    assertEquals("US", geo.lookupCountry(ip));
    assertEquals("715", geo.lookupASN(ip));
  }


  @Test
  public void lookupBytes() throws UnknownHostException {
    InetAddress address = InetAddresses.forString("74.80.89.0");

    assertEquals("DE", geo.lookupCountry(address));
    assertEquals("715", geo.lookupASN(address));

    // Both should be equivalent
    address = InetAddress.getByName("74.80.89.0");

    assertEquals("DE", geo.lookupCountry(address));
    assertEquals("715", geo.lookupASN(address));
  }

  @Test
  public void testLookupAsn() {
    for (String ip : Lists.newArrayList("185.20.63.0", "2001:0500:0015:0600::")) {
      InetAddress address = InetAddresses.forString(ip);

      String byString = geo.lookupASN(ip);
      String byAddress = geo.lookupASN(address);

      System.out.println("byString = " + byString);
      System.out.println("byAddress = " + byAddress);

      assertEquals(byAddress, byString);
    }
  }

  @Test
  public void testLookupCountry() {
    String ip = "185.20.63.0";
    InetAddress address = InetAddresses.forString(ip);

    String byString = geo.lookupCountry(ip);
    String byAddress = geo.lookupCountry(address);

    System.out.println("byString = " + byString);
    System.out.println("byAddress = " + byAddress);

    assertEquals(byAddress, byString);
  }

}

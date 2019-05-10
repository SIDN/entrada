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

import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CountryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Utility class to lookup IP adress information such as country and asn. Uses the maxmind database
 */
public class GeoLookupUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoLookupUtil.class);

  private static final String MAXMIND_DIR = "maxmind";
  private static final String MAXMIND_COUNTRY_DB = "GeoLite2-Country.mmdb";
  private static final String MAXMIND_ASN_DB = "GeoLite2-ASN.mmdb";

  private DatabaseReader geoReader;
  private DatabaseReader asnReader;

  public GeoLookupUtil() {
    Settings settings = Settings.getInstance();
    String path = settings.getSetting(Settings.STATE_LOCATION)
        + System.getProperty("file.separator") + MAXMIND_DIR + System.getProperty("file.separator");
    try {
      // geo
      File database = new File(path + MAXMIND_COUNTRY_DB);
      geoReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
      // asn
      database = new File(path + MAXMIND_ASN_DB);
      asnReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
    } catch (IOException e) {
      throw new RuntimeException("Error initializing Maxmind GEO/ASN database", e);
    }
  }

  public String lookupCountry(String ip) {
    InetAddress inetAddr;
    try {
      inetAddr = InetAddresses.forString(ip);
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Invalid IP address: " + ip);
      }
      return null;
    }
    return lookupCountry(inetAddr);
  }

  public String lookupCountry(InetAddress addr) {
    CountryResponse response;

    try {
      response = geoReader.country(addr);
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No country found for: " + addr);
      }
      return null;
    }
    return response.getCountry().getIsoCode();
  }

  public String lookupASN(InetAddress ip) {
    try {
      AsnResponse ar = asnReader.asn(ip);
      return String.valueOf(ar.getAutonomousSystemNumber());
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No asn found for: " + ip);
      }
    }
    return null;
  }

  public String lookupASN(String ip) {
    InetAddress inetAddr;
    try {
      inetAddr = InetAddresses.forString(ip);
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Invalid IP address: " + ip);
      }
      return null;
    }
    return lookupASN(inetAddr);
  }

}

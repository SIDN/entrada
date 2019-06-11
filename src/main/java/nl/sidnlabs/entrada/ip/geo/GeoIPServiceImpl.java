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
package nl.sidnlabs.entrada.ip.geo;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;

/**
 * Utility class to lookup IP adress information such as country and asn. Uses the maxmind database
 */
@Log4j2
@Component
public class GeoIPServiceImpl implements GeoIPService {

  private static final String MAXMIND_COUNTRY_DB = "GeoLite2-Country.mmdb";
  private static final String MAXMIND_ASN_DB = "GeoLite2-ASN.mmdb";

  private DatabaseReader geoReader;
  private DatabaseReader asnReader;

  public GeoIPServiceImpl(@Value("${geoip.maxmind.location}") String location) {

    try {
      // geo
      File database = new File(new File(StringUtils.removeStart(location, "file://"))
          + System.getProperty("file.separator") + MAXMIND_COUNTRY_DB);

      geoReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
      // asn
      database = new File(StringUtils.removeStart(location, "file://")
          + System.getProperty("file.separator") + MAXMIND_ASN_DB);
      asnReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
    } catch (IOException e) {
      throw new ApplicationException("Error initializing Maxmind GEO/ASN database", e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see nl.sidn.pcap.ip.geo.GeoIPService#lookupCountry(java.lang.String)
   */
  @Override
  public Optional<String> lookupCountry(String ip) {
    InetAddress inetAddr;
    try {
      inetAddr = InetAddresses.forString(ip);
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Invalid IP address: " + ip);
      }
      return Optional.empty();
    }
    return lookupCountry(inetAddr);
  }

  /*
   * (non-Javadoc)
   * 
   * @see nl.sidn.pcap.ip.geo.GeoIPService#lookupCountry(java.net.InetAddress)
   */
  @Override
  public Optional<String> lookupCountry(InetAddress addr) {
    try {
      return Optional.ofNullable(geoReader.country(addr).getCountry().getIsoCode());
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("No country found for: " + addr);
      }
    }
    return Optional.empty();
  }

  /*
   * (non-Javadoc)
   * 
   * @see nl.sidn.pcap.ip.geo.GeoIPService#lookupASN(java.net.InetAddress)
   */
  @Override
  public Optional<String> lookupASN(InetAddress ip) {
    try {
      AsnResponse ar = asnReader.asn(ip);
      return Optional.ofNullable(String.valueOf(ar.getAutonomousSystemNumber()));
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("No asn found for: " + ip);
      }
    }
    return Optional.empty();
  }

  /*
   * (non-Javadoc)
   * 
   * @see nl.sidn.pcap.ip.geo.GeoIPService#lookupASN(java.lang.String)
   */
  @Override
  public Optional<String> lookupASN(String ip) {
    InetAddress inetAddr;
    try {
      inetAddr = InetAddresses.forString(ip);
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Invalid IP address: " + ip);
      }
      return Optional.empty();
    }
    return lookupASN(inetAddr);
  }

}

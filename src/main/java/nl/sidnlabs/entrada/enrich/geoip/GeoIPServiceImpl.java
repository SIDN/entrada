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
package nl.sidnlabs.entrada.enrich.geoip;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Optional;
import javax.annotation.PostConstruct;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.google.common.net.InetAddresses;
import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.IspResponse;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.util.DownloadUtil;
import nl.sidnlabs.entrada.util.FileUtil;

/**
 * Utility class to lookup IP adress information such as country and asn. Uses the maxmind database
 */
@Log4j2
@Component
public class GeoIPServiceImpl implements GeoIPService {

  // free dbs
  private static final String FILENAME_GEOLITE_COUNTRY = "GeoLite2-Country.mmdb";
  private static final String FILENAME_GEOLITE_ASN = "GeoLite2-ASN.mmdb";
  // paid dbs
  private static final String FILENAME_GEOIP2_COUNTRY = "GeoIP2-Country.mmdb";
  private static final String FILENAME_GEOIP2_ASN = "GeoIP2-ISP.mmdb";


  @Value("${geoip.maxmind.location}")
  private String location;
  @Value("${geoip.maxmind.age.max}")
  private int maxAge;

  @Value("${geoip.maxmind.url.country}")
  private String urlCountryDb;
  @Value("${geoip.maxmind.url.asn}")
  private String urlAsnDb;
  @Value("${geoip.maxmind.url.country.paid}")
  private String urlCountryDbPaid;
  @Value("${geoip.maxmind.url.asn.paid}")
  private String urlAsnDbPaid;

  @Value("${geoip.maxmind.subsciption.key}")
  private String licenseKey;

  private boolean usePaidVersion;

  private DatabaseReader geoReader;
  private DatabaseReader asnReader;

  @PostConstruct
  public void initialize() {

    usePaidVersion = StringUtils.isNotBlank(licenseKey);

    File loc = new File(location);
    if (!loc.exists()) {
      loc.mkdirs();
    }

    String countryFile = countryFile();
    String asnFile = asnFile();

    if (shouldUpdate(countryFile)) {
      log.info("GEOIP country database does not exist or is too old, fetch latest version");
      String url = urlCountryDb;
      if (usePaidVersion) {
        log.info("Download paid Maxmind country database");
        url = urlCountryDbPaid + licenseKey;
      }
      download(countryFile, url, 30);
    }


    if (shouldUpdate(asnFile)) {
      log.info("GEOIP ASN database does not exist or is too old, fetch latest version");
      String url = urlAsnDb;
      if (usePaidVersion) {
        log.info("Download paid Maxmind ISP database");
        url = urlAsnDbPaid + licenseKey;
      }
      download(asnFile, url, 30);
    }

    try {
      // geo
      File database = new File(FileUtil.appendPath(location, countryFile));
      geoReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
      // asn
      database = new File(FileUtil.appendPath(location, asnFile));
      asnReader = new DatabaseReader.Builder(database).withCache(new CHMCache()).build();
    } catch (IOException e) {
      throw new ApplicationException("Error initializing Maxmind GEO/ASN database", e);
    }
  }

  private String countryFile() {
    return usePaidVersion ? FILENAME_GEOIP2_COUNTRY : FILENAME_GEOLITE_COUNTRY;
  }

  private String asnFile() {
    return usePaidVersion ? FILENAME_GEOIP2_ASN : FILENAME_GEOLITE_ASN;
  }

  /**
   * Check if the database should be updated
   * 
   * @param database named of database
   * @return true if database file does not exist or is too old
   */
  private boolean shouldUpdate(String database) {
    File f = new File(FileUtil.appendPath(location, database));
    return !f.exists() || isExpired(f, maxAge);
  }

  private boolean isExpired(File f, int max) {
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1 * max);
    return FileUtils.isFileOlder(f, calendar.getTime());
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
  public Optional<String> lookupCountry(InetAddress ip) {
    try {
      return Optional.ofNullable(geoReader.country(ip).getCountry().getIsoCode());
    } catch (AddressNotFoundException e) {
      if (log.isDebugEnabled()) {
        log.debug("Maxmind error, IP not in database: " + ip);
      }
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("No country found for: " + ip);
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
      if (usePaidVersion) {
        // paid version returns IspResponse
        IspResponse r = asnReader.isp(ip);
        return asn(r.getAutonomousSystemNumber(), ip);
      }
      // use free version
      AsnResponse r = asnReader.asn(ip);
      return asn(r.getAutonomousSystemNumber(), ip);
    } catch (AddressNotFoundException e) {
      if (log.isDebugEnabled()) {
        log.debug("Maxmind error, IP not in database: " + ip);
      }
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Error while doing ASN lookup for: " + ip);
      }
    }
    return Optional.empty();
  }

  public Optional<String> asn(Integer asn, InetAddress ip) {
    if (asn == null) {
      if (log.isDebugEnabled()) {
        log.debug("No asn found for: " + ip);
      }
      return Optional.empty();
    }
    return Optional.ofNullable(asn.toString());
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


  public boolean download(String database, String url, int timeout) {
    Optional<byte[]> data = DownloadUtil.getAsBytes(url, timeout);
    if (data.isPresent()) {
      InputStream is = new ByteArrayInputStream(data.get());
      try {
        extractDatabase(is, database);
      } catch (IOException e) {
        log.error("Error while extracting {}", database, e);
        return false;
      }
    }

    return true;
  }

  private void extractDatabase(InputStream in, String database) throws IOException {
    GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in);
    try (TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {
      TarArchiveEntry entry;

      while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
        if (StringUtils.endsWith(entry.getName(), database)) {
          int count;
          byte[] data = new byte[4096];

          String outFile = Paths.get(entry.getName()).getFileName().toString();
          FileOutputStream fos =
              new FileOutputStream(FileUtil.appendPath(location, outFile), false);
          try (BufferedOutputStream dest = new BufferedOutputStream(fos, 4096)) {
            while ((count = tarIn.read(data, 0, 4096)) != -1) {
              dest.write(data, 0, count);
            }
          }
        }
      }
    }
  }


}

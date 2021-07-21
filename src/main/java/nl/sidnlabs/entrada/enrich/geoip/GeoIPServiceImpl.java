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
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader.FileMode;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CountryResponse;
import lombok.extern.log4j.Log4j2;
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

  private static final int DEFAULT_CACHE_SIZE = 1024 * 1000;

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

  @Value("${geoip.maxmind.license.free}")
  private String licenseKeyFree;

  @Value("${geoip.maxmind.license.paid}")
  private String licenseKeyPaid;

  @Value("${geoip.maxmind.location}")
  private String location;

  private DatabaseReader geoReader;
  private DatabaseReader asnReader;

  private boolean usePaidVersion;
  private boolean geoDbInitialised;

  @PostConstruct
  public void initialize() {

    log.info("Using Maxmind database location: {}", location);
    if (StringUtils.isBlank(licenseKeyFree) && StringUtils.isBlank(licenseKeyPaid)) {
      throw new RuntimeException(
          "No valid Maxmind license key found, provide key for either the free of paid license.");
    }

    usePaidVersion = StringUtils.isNotBlank(licenseKeyPaid);

    File loc = new File(location);
    if (!loc.exists()) {
      loc.mkdirs();
    }

    String countryFile = countryFile();
    String asnFile = asnFile();

    if (shouldUpdate(countryFile)) {
      log.info("GEOIP country database does not exist or is too old, fetch latest version");
      String url = urlCountryDb + licenseKeyFree;
      if (usePaidVersion) {
        log.info("Download paid Maxmind country database");
        url = urlCountryDbPaid + licenseKeyPaid;
      }
      download(countryFile, url, 30);
      geoDbInitialised = false;
    }


    if (shouldUpdate(asnFile)) {
      log.info("GEOIP ASN database does not exist or is too old, fetch latest version");
      String url = urlAsnDb + licenseKeyFree;
      if (usePaidVersion) {
        log.info("Download paid Maxmind ISP database");
        url = urlAsnDbPaid + licenseKeyPaid;
      }
      download(asnFile, url, 30);
      geoDbInitialised = false;
    }

    try {
      // geo
      if (!geoDbInitialised) {
        File database = new File(FileUtil.appendPath(location, countryFile));
        geoReader = new DatabaseReader.Builder(database)
            .withCache(new CHMCache(DEFAULT_CACHE_SIZE))
            .fileMode(FileMode.MEMORY)
            .build();
        // asn
        database = new File(FileUtil.appendPath(location, asnFile));
        asnReader = new DatabaseReader.Builder(database)
            .withCache(new CHMCache(DEFAULT_CACHE_SIZE))
            .fileMode(FileMode.MEMORY)
            .build();
        geoDbInitialised = true;
      }
    } catch (IOException e) {
      throw new RuntimeException("Error initializing Maxmind GEO/ASN database", e);
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

    if (log.isDebugEnabled()) {
      log.debug("Check for file expiration for: {}", f);
    }
    if (!f.exists()) {
      log.info("File does not exist: {}", f);
      return true;
    }

    if (isExpired(f, maxAge)) {
      log.info("File is expired: {}", f);
      return true;
    }

    return false;
  }

  private boolean isExpired(File f, int max) {
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1 * max);
    return FileUtils.isFileOlder(f, calendar.getTime());
  }

  /*
   * (non-Javadoc)
   * 
   * @see nl.sidn.pcap.ip.geo.GeoIPService#lookupCountry(java.net.InetAddress)
   */
  @Override
  public Optional<CountryResponse> lookupCountry(InetAddress ip) {
    try {
      return geoReader.tryCountry(ip);
    } catch (Exception e) {
      log.error("Maxmind lookup error for: {}", ip, e);
    }
    return Optional.empty();
  }

  /*
   * (non-Javadoc)
   * 
   * @see nl.sidn.pcap.ip.geo.GeoIPService#lookupASN(java.net.InetAddress)
   */
  @Override
  public Optional<? extends AsnResponse> lookupASN(InetAddress ip) {
    try {
      if (usePaidVersion) {
        // paid version returns IspResponse
        return asnReader.tryIsp(ip);
      }

      // use free version
      return asnReader.tryAsn(ip);

    } catch (Exception e) {
      log.error("Maxmind error for IP: {}", ip, e);
    }

    return Optional.empty();
  }

  public boolean download(String database, String url, int timeout) {
    // do not log api key
    String logUrl = RegExUtils.removePattern(url, "&license_key=.+");
    Optional<byte[]> data = DownloadUtil.getAsBytes(url, logUrl, timeout);
    if (data.isPresent()) {
      InputStream is = new ByteArrayInputStream(data.get());
      try {
        extractDatabase(is, database);
      } catch (IOException e) {
        log.error("Error while extracting {}", database, e);
        return false;
      }

      return true;
    }

    // no data could be downloaded
    return false;
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

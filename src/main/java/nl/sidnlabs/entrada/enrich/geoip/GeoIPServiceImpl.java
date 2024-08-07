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
import java.util.Date;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
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
    
    String url = urlCountryDb + licenseKeyFree;
    if (usePaidVersion) {
      url = urlCountryDbPaid + licenseKeyPaid;
    }

    if (shouldUpdate(countryFile, url)) {
      log.info("GEOIP country database does not exist or is too old, fetch latest version");
     
      download(countryFile, url, 30);
      geoDbInitialised = false;
    }


    url = urlAsnDb + licenseKeyFree;
    if (usePaidVersion) {
      url = urlAsnDbPaid + licenseKeyPaid;
    }
    
    if (shouldUpdate(asnFile, url)) {
      log.info("GEOIP ASN database does not exist or is too old, fetch latest version");
      
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
  private boolean shouldUpdate(String database, String url) {
    File f = new File(FileUtil.appendPath(location, database));

    if (log.isDebugEnabled()) {
      log.debug("Check for file expiration for: {}", f);
    }
   
    // no file downlaoded yet, download for first time
    if (!f.exists()) {
      log.info("Database file {} does not exist", f);
      return true;
    }
   
    // check if there is online update for local file
    Date lastModified = lastModifiedOnline(url, 30);
    return lastModified != null && lastModified.after(new Date(f.lastModified()));
  }
  
  public Date lastModifiedOnline(String url, int timeout) {

	    try (CloseableHttpClient client =
	        HttpClientBuilder.create().setDefaultRequestConfig(createConfig(timeout * 1000)).build()){
	    	     
	    	try(CloseableHttpResponse response = client.execute(new HttpHead(url))){
		      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
		    	if(log.isDebugEnabled()) {
		    		log.debug("Maxmind API HEAD request status is OK, last-modified header value = " + response.getFirstHeader("last-modified").getValue());
		    	}
		    	return  DateUtils.parseDate(response.getFirstHeader("last-modified").getValue());
		      }else  if (response.getStatusLine().getStatusCode() == HttpStatus.SC_TOO_MANY_REQUESTS) {
		    	  log.error("Maxmind API Ratelimiting error, make sure to limit the number of requests to the Maxmind API. ");
		      }else {
		         log.error("Maxmind API HEAD request failed, status code: " + response.getStatusLine().getStatusCode());
		      }
	      }
	    } catch (Exception e) {
	      log.error("Errror executing HTTP HEAD request" + e);
	    }

	    return null;
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
  
  

  
 
  private static RequestConfig createConfig(int timeoutMillis) {
	    return RequestConfig
	        .custom()
	        // timeout for waiting during creating of connection
	        .setConnectTimeout(timeoutMillis)
	        .setConnectionRequestTimeout(timeoutMillis)
	        .setSocketTimeout(timeoutMillis)
	        // socket has timeout, for slow senders
	        .setSocketTimeout(timeoutMillis)
	        // do not let the apache http client initiate redirects
	        // build it
	        .build();
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

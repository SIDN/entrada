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
package nl.sidnlabs.pcap.dns.resolver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.time.DateUtils;
import com.google.common.net.InetAddresses;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.pcap.config.Settings;

@Log4j2
public abstract class AbstractNetworkCheck implements DnsResolverAddress {

  protected List<Subnet> bitSubnets = new ArrayList<>();
  protected List<String> subnets = new ArrayList<>();

  private Map<String, Boolean> matchCache = new HashMap<>();

  protected Settings settings;

  public AbstractNetworkCheck(Settings settings) {
    this.settings = settings;

    String filename = settings.getStateDir() + System.getProperty("file.separator") + getFilename();
    File file = new File(filename);
    // if file does not exist or was update last on previous day, then update resolvers ip's
    if (file.exists()) {
      // read from local file
      try {
        readFromFile(file);
      } catch (IOException e) {
        log.debug("File {} cannot be read, reload from url as backup", file, e);
        PreInit(file);
      }
    }
  }

  public void update() {
    String filename = settings.getStateDir() + System.getProperty("file.separator") + getFilename();
    File file = new File(filename);
    // if file does not exist or was update last on previous day, then update resolvers ip's
    if (file.exists()) {
      Date lastModifiedDate = new Date(file.lastModified());
      Date currentDate = new Date();

      if (!DateUtils.isSameDay(lastModifiedDate, currentDate)) {
        if (log.isDebugEnabled()) {
          log.debug("File {} is too old, updating now.", filename);
        }
        PreInit(file);
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("File {} does not exist, fetching now.", filename);
      }
      PreInit(file);
    }

  }

  private void PreInit(File file) {
    try {
      init();
    } catch (Exception e) {
      log.error("Error while processing resolver addresses", e);
      // stop here, do not write new file with partial data
      return;
    }
    if (subnets.size() > 0) {
      try {
        writeToFile(file);
      } catch (IOException e) {
        log.debug("File {} cannot be written", file, e);
      }
    }

  }

  protected abstract void init();

  protected void readFromFile(File file) throws IOException {
    log.info("Load resolver addresses from file: " + file);
    String sCurrentLine;
    BufferedReader br = new BufferedReader(new FileReader(file));

    while ((sCurrentLine = br.readLine()) != null) {
      try {
        if (log.isDebugEnabled()) {
          log.debug("Read resolver {} from file: {} ", sCurrentLine, file.getAbsolutePath());
        }
        bitSubnets.add(Subnet.createInstance(sCurrentLine));
      } catch (UnknownHostException e) {
        log.error("Problem while adding Google resolver IP range: " + sCurrentLine + e);
      }
    }

    br.close();
  }

  private void writeToFile(File file) throws IOException {
    if (!file.exists()) {
      file.createNewFile();
    }

    String sep = System.lineSeparator();
    FileWriter fw = new FileWriter(file.getAbsoluteFile(), false);
    BufferedWriter bw = new BufferedWriter(fw);
    for (String range : subnets) {
      if (log.isDebugEnabled()) {
        log.debug("Write resolver {} to file: {} ", range, file.getAbsolutePath());
      }
      bw.write(range);
      bw.write(sep);
    }
    bw.close();
  }

  protected abstract String getFilename();

  @Override
  public boolean isMatch(String address) {
    Boolean cacheHit = matchCache.get(address);
    if (cacheHit != null) {
      return cacheHit.booleanValue();
    }
    InetAddress ipAddress = InetAddresses.forString(address);
    boolean match = bitCompare(ipAddress);

    // create cache with hashmap for matches for perf
    matchCache.put(address, match);

    return match;
  }

  private boolean bitCompare(InetAddress ipAddress) {
    for (Subnet sn : bitSubnets) {
      if (sn.isInNet(ipAddress)) {
        return true;
      }
    }

    return false;
  }

  public int getSize() {
    return bitSubnets.size();
  }

}

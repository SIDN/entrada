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
package nl.sidnlabs.entrada.enrich.resolver;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.jboss.netty.handler.ipfilter.IpSubnet;
import org.springframework.beans.factory.annotation.Value;
import com.google.common.net.InetAddresses;
import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class AbstractResolverCheck implements DnsResolverCheck {

  private List<IpSubnet> matchers4 = new ArrayList<>();
  private List<IpSubnet> matchers6 = new ArrayList<>();

  @Value("${entrada.location.persistence}")
  private String workDir;

  @Value("${public-resolver.match.cache.size:5000}")
  private int maxMatchCacheSize;

  // only cache the matches (hits) the non-matches are too numerous
  // to efficiently cache in memory, doing the subnet check would be faster
  // than checking 100k cached elements
  private Set<String> hitCache = new HashSet<>();

  @PostConstruct
  public void init() {

    String filename = workDir + System.getProperty("file.separator") + getFilename();
    File file = new File(filename);
    // read from local file
    if (!readFromFile(file)) {
      log.info("File {} cannot be read, reload from source as backup", file);
      update(file);
    }

    hitCache.clear();
  }

  private void update(File file) {
    // load new subnets from source
    List<String> subnets = fetch();
    if (!subnets.isEmpty()) {
      matchers4.clear();
      matchers6.clear();
      // create subnets for matching
      subnets
          .stream()
          .filter(s -> s.contains("."))
          .map(this::subnetFor)
          .filter(Objects::nonNull)
          .forEach(s -> matchers4.add(s));

      subnets
          .stream()
          .filter(s -> s.contains(":"))
          .map(this::subnetFor)
          .filter(Objects::nonNull)
          .forEach(s -> matchers6.add(s));

      // write subnets to file so we do not need to get them from source every time the app starts
      writeToFile(subnets, file);
    }
    log.info("Loaded {} resolver addresses from file: {}", getMatchers(), file);
  }

  private IpSubnet subnetFor(String address) {
    try {
      return new IpSubnet(address);
    } catch (UnknownHostException e) {
      log.error("Cannot create subnet for: {}", address, e);
    }

    return null;
  }

  protected abstract List<String> fetch();

  private boolean readFromFile(File file) {
    log.info("Load resolver addresses from file: " + file);

    // if file does not exist or was update last on previous day, then update resolvers ip's
    if (!file.exists()) {
      return false;
    }

    Date lastModifiedDate = new Date(file.lastModified());
    Date currentDate = new Date();

    if (!DateUtils.isSameDay(lastModifiedDate, currentDate)) {
      if (log.isDebugEnabled()) {
        log.debug("File {} is too old, do not use it.", file);
      }
      return false;
    }

    List<String> lines;
    try {
      lines = Files.readAllLines(file.toPath());
    } catch (Exception e) {
      log.error("Error while reading file: {}", file, e);
      return false;
    }
    matchers4.clear();
    matchers6.clear();

    lines
        .stream()
        .filter(s -> s.contains("."))
        .map(this::subnetFor)
        .filter(Objects::nonNull)
        .forEach(s -> matchers4.add(s));

    lines
        .stream()
        .filter(s -> s.contains(":"))
        .map(this::subnetFor)
        .filter(Objects::nonNull)
        .forEach(s -> matchers6.add(s));

    log.info("Loaded {} resolver addresses from file: {}", getMatchers(), file);

    return !matchers4.isEmpty() || !matchers6.isEmpty();
  }

  private void writeToFile(List<String> subnets, File file) {
    try {
      Files.write(file.toPath(), subnets, CREATE, TRUNCATE_EXISTING);
    } catch (IOException e) {
      log.error("Problem while writing to file: {}", file);
    }
  }

  protected abstract String getFilename();

  @Override
  public boolean match(String address) {

    if (hitCache.contains(address)) {
      return true;
    }

    InetAddress ip = ip(address);
    if (ip == null) {
      // not a valid ip
      // addToCache(hash, false);
      return false;
    }

    // do subnet matching
    // matchers are split into a v4 and v6 group
    // to allow us to quickly skip the version we don't need to check

    if (StringUtils.contains(address, ".")) {
      // do v4 check only
      for (IpSubnet sn : matchers4) {
        if (sn.contains(ip)) {
          addToCache(address);
          return true;
        }
      }
    } else {
      // do v6 check only
      for (IpSubnet sn : matchers6) {
        if (sn.contains(ip)) {
          addToCache(address);
          return true;
        }
      }
    }

    // address not a known resolver
    return false;
  }

  private InetAddress ip(String address) {
    try {
      return InetAddresses.forString(address);
    } catch (Exception e) {
      // ignore
      if (log.isDebugEnabled()) {
        log.debug("Invalid IP address {}", address);
      }
    }

    return null;
  }

  private void addToCache(String address) {
    if (hitCache.size() >= maxMatchCacheSize) {
      log.info("{} resolver match cache limit reached", getName(), maxMatchCacheSize);
      hitCache.clear();
    }
    hitCache.add(address);
  }

  @Override
  public int getMatchers() {
    return matchers4.size() + matchers6.size();
  }

  @Override
  public void done() {
    if (log.isDebugEnabled()) {
      log.debug("{} Clear match cache, size={}", getName(), hitCache.size());
    }
    hitCache.clear();
  }

}

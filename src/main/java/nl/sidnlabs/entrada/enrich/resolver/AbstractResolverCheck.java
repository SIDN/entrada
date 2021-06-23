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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.springframework.beans.factory.annotation.Value;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Data
public abstract class AbstractResolverCheck implements DnsResolverCheck {

  private static final int BLOOMFILTER_IPV6_MAX = 50000;

  private List<FastIpSubnet> matchers4 = new ArrayList<>();
  private List<FastIpSubnet> matchers6 = new ArrayList<>();

  @Value("${entrada.location.persistence}")
  private String workDir;

  @Value("${public-resolver.match.cache.size:10000}")
  private int maxMatchCacheSize;

  private Cache<InetAddress, Boolean> hitCache;

  private BloomFilter<Long> ipv4Filter;
  private BloomFilter<String> ipv6SeenFilter;
  private BloomFilter<String> ipv6NegativeFilter;

  public void init() {

    String filename = workDir + System.getProperty("file.separator") + getFilename();
    File file = new File(filename);
    // read from local file
    if (!readFromFile(file)) {
      log.info("File {} cannot be read, reload from source as backup", file);
      update(file);
    }

    ipv6SeenFilter =
        BloomFilter.create(Funnels.stringFunnel(Charsets.US_ASCII), BLOOMFILTER_IPV6_MAX, 0.01);

    ipv6NegativeFilter =
        BloomFilter.create(Funnels.stringFunnel(Charsets.US_ASCII), BLOOMFILTER_IPV6_MAX, 0.01);

    if (hitCache == null) {
      hitCache = new Cache2kBuilder<InetAddress, Boolean>() {}
          .name(getName() + "-resolver-cache")
          .entryCapacity(maxMatchCacheSize)
          .build();
    }
  }

  private void createIpV4BloomFilter(List<String> subnets) {
    Set<Long> ipv4Set = new HashSet<>();
    if (!subnets.isEmpty()) {

      for (String sn : subnets) {
        if (sn.contains(".")) {
          String cidr = StringUtils.substringAfter(sn, "/");
          if (NumberUtils.isCreatable(cidr)) {
            SubnetUtils utils = new SubnetUtils(sn);
            for (String ip : utils.getInfo().getAllAddresses()) {
              ipv4Set.add(Long.valueOf(ipToLong(ip)));
            }
          } else {
            log.info("Not adding invalid subnet {} to IPv4 bloomfilter", sn);
          }
        }
      }
    }

    ipv4Filter = BloomFilter.create(Funnels.longFunnel(), ipv4Set.size(), 0.01);

    for (Long addr : ipv4Set) {
      ipv4Filter.put(addr);
    }

    log.info("Created IPv4 filter table with size: {}", ipv4Set.size());
  }


  public long ipToLong(String ipAddress) {

    String[] ipAddressInArray = StringUtils.split(ipAddress, ".");

    long result = 0;
    for (int i = 0; i < ipAddressInArray.length; i++) {

      int power = 3 - i;
      int ip = Integer.parseInt(ipAddressInArray[i]);
      result += ip * Math.pow(256, power);

    }

    return result;
  }

  private void update(File file) {
    // load new subnets from source
    List<String> subnets = fetch();

    createIpV4BloomFilter(subnets);

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



  private FastIpSubnet subnetFor(String address) {
    try {
      return new FastIpSubnet(address);
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

    createIpV4BloomFilter(lines);

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
  public boolean match(InetAddress address) {

    Boolean value = hitCache.peek(address);
    if (value != null) {
      return true;
    }

    if (address instanceof Inet4Address) {
      // do v4 check only
      long addr = ipToLong(address.getHostAddress());

      if (ipv4Filter.mightContain(Long.valueOf(addr))) {
        return checkv4(address);
      }

      return false;

    }

    // do v6 check only
    String addr = address.getHostAddress();
    if (!ipv6SeenFilter.mightContain(addr)) {
      // ip not seen before, do check
      ipv6SeenFilter.put(addr);
      boolean isInRange = checkv6(address);
      if (isInRange) {
        ipv6NegativeFilter.put(addr);
      }
      return isInRange;
    } else if (!ipv6NegativeFilter.mightContain(addr)) {
      // ip MUST have been seen before and MUST not be in ipv6NegativeFilter
      return false;
    }

    // IP has been seen before and the check result MUST have been: True
    return true;
  }

  private boolean checkv4(InetAddress address) {
    for (FastIpSubnet sn : matchers4) {
      if (sn.contains(address)) {
        addToCache(address);
        return true;
      }
    }
    return false;
  }

  private boolean checkv6(InetAddress address) {
    for (FastIpSubnet sn : matchers6) {
      if (sn.contains(address)) {
        addToCache(address);
        return true;
      }
    }
    return false;
  }

  private void addToCache(InetAddress address) {
    hitCache.put(address, Boolean.TRUE);
  }

  @Override
  public int getMatchers() {
    return matchers4.size() + matchers6.size();
  }

  @Override
  public void done() {
    if (log.isDebugEnabled()) {
      log.debug("{} Clear match cache", getName());
    }
    hitCache.clear();
  }

}

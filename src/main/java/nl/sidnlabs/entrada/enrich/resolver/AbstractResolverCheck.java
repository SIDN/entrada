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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.net.util.SubnetUtils;
import org.springframework.beans.factory.annotation.Value;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.googlecode.ipv6.IPv6Address;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Getter
@Setter
public abstract class AbstractResolverCheck implements DnsResolverCheck {

  private List<FastIpSubnet> matchers4 = new ArrayList<>();
  private List<FastIpV6Subnet> matchers6 = new ArrayList<>();

  @Value("${entrada.location.persistence}")
  private String workDir;

  @Value("${public-resolver.match.cache.size:10000}")
  private int maxMatchCacheSize;

  private BloomFilter<String> ipv4Filter;

  public void init() {

    String filename = workDir + System.getProperty("file.separator") + getFilename();
    File file = new File(filename);
    // read from local file
    if (!isFileAvailable(file)) {
      log.info("Fetch new data for {}", getName());
      update(file);
    }

    load(file);
  }

  private void createIpV4BloomFilter(List<String> subnets) {
    Set<String> ipv4Set = new HashSet<>();
    if (!subnets.isEmpty()) {

      for (String sn : subnets) {
        if (sn.contains(".")) {
          String cidr = StringUtils.substringAfter(sn, "/");
          if (NumberUtils.isCreatable(cidr)) {
            SubnetUtils utils = new SubnetUtils(sn);
            for (String ip : utils.getInfo().getAllAddresses()) {
              ipv4Set.add(ip);
            }
          } else {
            log.info("Not adding invalid subnet {} to IPv4 bloomfilter", sn);
          }
        }
      }
    }

    ipv4Filter = BloomFilter.create(Funnels.stringFunnel(Charsets.US_ASCII), ipv4Set.size(), 0.01);

    for (String addr : ipv4Set) {
      ipv4Filter.put(addr);
    }

    log.info("Created IPv4 filter table with size: {}", ipv4Set.size());
  }

  private void update(File file) {
    // load new subnets from source
    List<String> subnets = fetch();

    if (!subnets.isEmpty()) {
      // write subnets to file so we do not need to get them from source every time the app starts
      writeToFile(subnets, file);
    }
    log.info("Fetched {} resolver addresses from file: {}", subnets.size(), file);
  }

  private void load(File file) {
    List<String> lines;
    try {
      lines = Files.readAllLines(file.toPath());
    } catch (Exception e) {
      log.error("Error while reading file: {}", file, e);
      return;
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
        .map(this::v6SubnetFor)
        .filter(Objects::nonNull)
        .forEach(s -> matchers6.add(s));

    log.info("Loaded {} resolver addresses from file: {}", getMatcherCount(), file);
  }

  private FastIpSubnet subnetFor(String address) {
    try {
      return new FastIpSubnet(address);
    } catch (UnknownHostException e) {
      log.error("Cannot create subnet for: {}", address, e);
    }

    return null;
  }

  private FastIpV6Subnet v6SubnetFor(String address) {
    try {
      return new FastIpV6Subnet(address);
    } catch (UnknownHostException e) {
      log.error("Cannot create subnet for: {}", address, e);
    }

    return null;
  }

  protected abstract List<String> fetch();

  private boolean isFileAvailable(File file) {
    log.info("Load resolver addresses from file: " + file);

    // if file does not exist or if it was not created today, then update resolvers ip's
    if (!file.exists()) {
      return false;
    }

    Date lastModifiedDate = new Date(file.lastModified());
    Date currentDate = new Date();

    if (!DateUtils.isSameDay(lastModifiedDate, currentDate)) {
      log.info("File {} is too old", file);
      return false;
    }

    return true;
  }

  private void writeToFile(List<String> subnets, File file) {
    try {
      Files.write(file.toPath(), subnets, CREATE, TRUNCATE_EXISTING);
    } catch (IOException e) {
      log.error("Problem while writing to file: {}", file);
    }
  }

  protected abstract String getFilename();


  private boolean isIpv4(String address) {
    return address.indexOf('.') != -1;
  }

  private boolean isIpv4InFilter(String address) {
    return ipv4Filter.mightContain(address);
  }

  @Override
  public boolean match(String address, InetAddress inetAddress) {

    if (isIpv4(address)) {
      // do v4 check only
      if (isIpv4InFilter(address)) {
        return checkv4(address, inetAddress);
      }

      return false;

    }

    // do v6 check only
    return checkv6(address, inetAddress);
  }

  private boolean checkv4(String address, InetAddress inetAddress) {
    for (FastIpSubnet sn : matchers4) {
      if (sn.contains(inetAddress)) {
        // addToCache(address);
        return true;
      }
    }
    return false;
  }

  private boolean checkv6(String address, InetAddress inetAddress) {
    IPv6Address v6 = IPv6Address.fromInetAddress(inetAddress);
    for (FastIpV6Subnet sn : matchers6) {
      if (sn.contains(v6)) {
        // addToCache(address);
        return true;
      }
    }
    return false;
  }

  @Override
  public int getMatcherCount() {
    return matchers4.size() + matchers6.size();
  }

  @Override
  public void done() {
    if (log.isDebugEnabled()) {
      log.debug("{} Clear match cache", getName());
    }

    ipv4Filter = null;
  }

}

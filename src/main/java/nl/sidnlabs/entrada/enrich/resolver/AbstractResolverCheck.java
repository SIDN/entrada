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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Value;
import com.google.common.net.InetAddresses;
import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class AbstractResolverCheck implements DnsResolverCheck {

  private List<Subnet> bitSubnets = new ArrayList<>();
  private List<String> subnets = new ArrayList<>();

  @Value("${entrada.work.dir}")
  private String workDir;

  @Override
  public void init() {

    String filename = workDir + System.getProperty("file.separator") + getFilename();
    File file = new File(filename);
    // read from local file
    if (!readFromFile(file)) {
      log.debug("File {} cannot be read, reload from source as backup", file);
      update(file);
    }
  }

  private void update(File file) {
    // load new subnets from source
    subnets = fetch();
    if (!subnets.isEmpty()) {
      // create internal bitsubnets for camparissions
      subnets.stream().forEach(s -> bitSubnets.add(Subnet.createInstance(s)));
      // write subnets to file so we do not need to get them from source every time the app starts
      writeToFile(file);
    }
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
    } catch (IOException e) {
      return false;
    }

    lines.stream().forEach(s -> bitSubnets.add(Subnet.createInstance(s)));

    return !bitSubnets.isEmpty();
  }

  private void writeToFile(File file) {
    try {
      Files.write(file.toPath(), subnets, CREATE, TRUNCATE_EXISTING);
    } catch (IOException e) {
      log.error("Problem while writing to file: {}", file);
    }
  }

  protected abstract String getFilename();

  @Override
  public boolean match(String address) {
    InetAddress ipAddress = InetAddresses.forString(address);
    return bitCompare(ipAddress);
  }

  private boolean bitCompare(InetAddress ipAddress) {
    return bitSubnets.stream().anyMatch(sn -> sn.isInNet(ipAddress));
  }

  @Override
  public int getSize() {
    return bitSubnets.size();
  }

}

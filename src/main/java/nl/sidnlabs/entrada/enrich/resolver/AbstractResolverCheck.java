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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.web.util.matcher.IpAddressMatcher;
import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class AbstractResolverCheck implements DnsResolverCheck {

  private List<IpAddressMatcher> matchers4 = new ArrayList<>();
  private List<IpAddressMatcher> matchers6 = new ArrayList<>();
  private List<String> subnets = new ArrayList<>();

  @Value("${entrada.location.work}")
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
      subnets
          .stream()
          .filter(s -> s.contains("."))
          .forEach(s -> matchers4.add(new IpAddressMatcher(s)));

      subnets
          .stream()
          .filter(s -> s.contains(":"))
          .forEach(s -> matchers6.add(new IpAddressMatcher(s)));

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

    lines
        .stream()
        .filter(s -> s.contains("."))
        .forEach(s -> matchers4.add(new IpAddressMatcher(s)));

    lines
        .stream()
        .filter(s -> s.contains(":"))
        .forEach(s -> matchers6.add(new IpAddressMatcher(s)));

    return !matchers4.isEmpty() || !matchers6.isEmpty();
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
    // reolver matchers are split into a v4 and v6 group
    // to allow us to quickly skip the version we don't need
    // to check

    if (address != null && address.contains(".")) {
      for (IpAddressMatcher m : matchers4) {
        if (m.matches(address)) {
          return true;
        }
      }
    }

    for (IpAddressMatcher m : matchers6) {
      if (m.matches(address)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public int getSize() {
    return matchers4.size() + matchers6.size();
  }

}

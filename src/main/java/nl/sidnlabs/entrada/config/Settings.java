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
package nl.sidnlabs.entrada.config;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.Data;
import nl.sidnlabs.dnslib.util.DomainParent;


@Data
@Component
public class Settings {

  private ServerInfo serverInfo = null;

  @Value("${entrada.tld.suffix}")
  private String tldSuffixConfig;
  private List<DomainParent> tldSuffixes = new ArrayList<>();

  private String inputDir;
  private String outputDir;
  private String stateDir;

  /**
   * Load the server and optional anycast server location information. Using format
   * {@code<server>_<location>}
   * 
   * @param name server name
   */
  public void setServer(String name) {
    this.serverInfo = new ServerInfo();
    // set the pcap input directory name.
    serverInfo.setFullname(name);
    if (name.contains("_")) {
      String[] parts = StringUtils.split(name, "_");
      if (parts.length == 2) {
        serverInfo.setName(parts[0]);
        serverInfo.setLocation(parts[1]);
        return;
      }
    }
    // no anycast location encoded in name
    serverInfo.setName(name);
  }

  private void createTldSuffixes() {
    tldSuffixes = new ArrayList<>();
    if (StringUtils.isEmpty(tldSuffixConfig)) {
      // no value found, do nothing
      return;
    }

    String[] tlds = StringUtils.split(tldSuffixConfig, ",");
    // create list of DomainParents
    for (int i = 0; i < tlds.length; i++) {
      String parent = tlds[i];
      if (parent == null) {
        // skip nulls
        continue;
      }
      // start and end with a dot.
      if (!StringUtils.startsWith(parent, ".")) {
        parent = "." + parent;
      }

      int labelCount = StringUtils.split(parent, '.').length;
      if (StringUtils.endsWith(parent, ".")) {
        // remove last dot (will become the used tld suffix
        tldSuffixes.add(new DomainParent(parent, StringUtils.removeEnd(parent, "."), labelCount));
      } else {
        tldSuffixes.add(new DomainParent(parent + ".", parent, labelCount));
      }
    }

  }

  public List<DomainParent> getTldSuffixes() {
    if (tldSuffixes == null) {
      createTldSuffixes();
    }
    return tldSuffixes;
  }
}

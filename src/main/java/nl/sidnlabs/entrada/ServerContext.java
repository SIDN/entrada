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
package nl.sidnlabs.entrada;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import lombok.Data;

@Data
@Component
public class ServerContext {

  private ServerInfo serverInfo = null;

  /**
   * Load the server and optional anycast server location information. Using format
   * {@code<server>_<location>}
   * 
   * @param name server name
   */
  public void setServer(String name) {
    serverInfo = new ServerInfo();

    String cleanName = StringUtils.stripToEmpty(name);
    serverInfo.setName(cleanName);
    serverInfo.setFullname(cleanName);
    serverInfo.setNormalizeName(cleanName.replaceAll("[^A-Za-z0-9 ]", "_"));

    if (cleanName.length() > 0 && cleanName.contains("_")) {
      String[] parts = StringUtils.split(cleanName, "_");
      if (parts.length == 2) {
        // found anyvast location in name, split into name and location part
        serverInfo.setName(parts[0]);
        serverInfo.setLocation(parts[1]);
      }
    }
  }


  @Data
  public static class ServerInfo {
    private String fullname;
    private String normalizeName;
    private String name;
    private String location;
  }
}

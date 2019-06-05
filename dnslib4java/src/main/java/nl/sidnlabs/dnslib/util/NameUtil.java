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
package nl.sidnlabs.dnslib.util;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class NameUtil {

  private NameUtil() {}

  private static final String DOMAIN_NAME_PATTERN =
      "^((?!-)[A-Za-z0-9-_]{1,63}(?<!-)\\.)*([A-Za-z]{2,2})*(\\.)*$";
  private static final Pattern FQDN = Pattern.compile(DOMAIN_NAME_PATTERN);

  public static Domaininfo getDomain(String name) {
    if (name == null || name.length() == 0) {
      return new Domaininfo(null, 0);
    }
    if (StringUtils.equals(name, ".")) {
      return new Domaininfo(name, 0);
    }

    String[] parts = StringUtils.split(name, ".");

    if (parts != null && parts.length > 0) {
      if (parts.length == 1) {
        // only 1 label present
        return new Domaininfo(parts[0], 1);
      }
      // all others have more than 1 label, get last 2
      return new Domaininfo(parts[parts.length - 2] + "." + parts[parts.length - 1], parts.length);
    }

    return new Domaininfo(name, 0);

  }

  public static Domaininfo getDomain(String name, List<DomainParent> parents) {
    // check if tld suffixes are loaded, if so use these
    if (parents != null && !parents.isEmpty()) {
      return getDomainWithSuffixList(name, parents);
    }
    // no suffixes use, simple old method of assuming tld is 1st level
    return getDomain(name);
  }

  public static Domaininfo getDomainWithSuffixList(String name, List<DomainParent> parents) {
    if (name == null || name.length() == 0) {
      return new Domaininfo(null, 0);
    }
    if (StringUtils.equals(name, ".")) {
      return new Domaininfo(name, 0);
    }

    String[] parts = StringUtils.split(name, ".");

    if (parts != null && parts.length > 0) {
      if (parts.length == 1) {
        // only 1 label present
        return new Domaininfo(parts[0], 1);
      }
      // try to find a matching tld suffix
      for (DomainParent parent : parents) {
        if (StringUtils.endsWith(name, parent.getMatch())) {
          // get the label before the tld which is the registred name.
          return new Domaininfo(parts[parts.length - (parent.getLabels() + 1)] + parent.getParent(),
              parts.length);
        }
      }

      // unknown tld, assume 2nd level tld
      return new Domaininfo(parts[parts.length - 2] + "." + parts[parts.length - 1], parts.length);
    }

    return new Domaininfo(name, 0);
  }



  public static boolean isFqdn(String domain) {
    return FQDN.matcher(domain).find();
  }

  public static String getSecondLevel(String name) {
    if (name == null || name.length() == 0) {
      return null;
    }

    String[] parts = StringUtils.split(name, '.');
    if (parts.length == 0) {
      return name;
    } else if (parts.length == 1) {
      return parts[parts.length - 1];
    } else {
      return parts[parts.length - 2] + "." + parts[parts.length - 1];
    }

  }

}

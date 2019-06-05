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

import org.apache.commons.lang3.StringUtils;

public class LabelUtil {

  private LabelUtil() {}

  public static int count(String name) {
    if (name == null || ".".equals(name)) {
      return 0;
    }

    return StringUtils.split(name, ".").length;

  }

  public static String stripFirstLabel(String name) {

    String[] parts = StringUtils.split(name, ".");
    if (parts != null && parts.length == 1) {
      return ".";
    } else if (parts != null && parts.length > 1) {
      StringBuilder b = new StringBuilder();
      boolean adddot = false;
      for (int i = 1; i < parts.length; i++) {
        if (adddot) {
          b.append(".");
        }
        b.append(parts[i]);

        adddot = true;

      }
      if (name.endsWith(".")) {
        b.append(".");
      }
      return b.toString();
    }

    return null;

  }

}

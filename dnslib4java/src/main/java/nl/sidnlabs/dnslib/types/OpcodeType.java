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
package nl.sidnlabs.dnslib.types;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 
 * DNS OpCodes
 * 
 * Registration Procedures Standards Action as modified by [RFC4020] Reference
 * [RFC-ietf-dnsext-rfc6195bis-05][RFC1035] OpCode Name Reference 0 Query [RFC1035] 1 IQuery
 * (Inverse Query, OBSOLETE) [RFC3425] 2 Status [RFC1035] 3 Unassigned 4 Notify [RFC1996] 5 Update
 * [RFC2136] 6-15 Unassigned
 *
 * 
 */
public enum OpcodeType {

  STANDARD(0), INVERSE(1), STATUS(2), UNASSIGNED_3(3), NOTIFY(4), UPPDATE(5), UNASSIGNED(-1);

  private int value;

  private static Map<String, OpcodeType> types = new HashMap<>();
  private static Map<Integer, OpcodeType> typesToInt = new HashMap<>();

  static {
    OpcodeType[] values = values();
    for (OpcodeType type : values) {
      types.put(type.name(), type);
      typesToInt.put(new Integer(type.getValue()), type);
    }
  }

  public static OpcodeType fromString(String name) {
    return types.get(StringUtils.upperCase(name));
  }

  public static OpcodeType fromValue(int value) {
    OpcodeType type = typesToInt.get(Integer.valueOf(value));
    if (type == null) {
      return UNASSIGNED;
    }

    return type;
  }

  private OpcodeType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }


}

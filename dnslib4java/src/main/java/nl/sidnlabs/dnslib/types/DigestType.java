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

/**
 * Digest Algorithms
 * 
 * Registration Procedures Standards Action Reference [RFC3658][RFC4034][RFC4035] Value Description
 * Status Reference 0 Reserved - [RFC3658] 1 SHA-1 MANDATORY [RFC3658] 2 SHA-256 MANDATORY [RFC4509]
 * 3 GOST R 34.11-94 OPTIONAL [RFC5933] 4 SHA-384 OPTIONAL [RFC6605] 5-255 Unassigned -
 *
 * 
 */
public enum DigestType {

  RESERVED((byte) 0, null), SHA1((byte) 1, "SHA-1"), SHA256((byte) 2, "SHA-256");

  private byte value;
  private String name;

  private DigestType(byte value, String name) {
    this.value = value;
    this.name = name;
  }

  private static Map<Integer, DigestType> types = new HashMap<>();

  static {
    DigestType[] values = values();
    for (DigestType type : values) {
      types.put(Integer.valueOf(type.getValue()), type);
    }
  }

  public byte getValue() {
    return value;
  }

  public static DigestType fromValue(short value) {
    return types.get(Integer.valueOf(value));
  }

  public String getName() {
    return name;
  }


}

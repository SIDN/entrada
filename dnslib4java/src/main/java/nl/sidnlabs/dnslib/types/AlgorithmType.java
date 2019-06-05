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

public enum AlgorithmType {

  /*
   * Zone Value Algorithm [Mnemonic] Signing References Status ----- -------------------- ---------
   * ---------- --------- 0 reserved 1 RSA/MD5 [RSAMD5] n [RFC2537] NOT RECOMMENDED 2 Diffie-Hellman
   * [DH] n [RFC2539] - 3 DSA/SHA-1 [DSA] y [RFC2536] OPTIONAL 4 Elliptic Curve [ECC] TBA - 5
   * RSA/SHA-1 [RSASHA1] y [RFC3110] MANDATORY 8 RSA/SHA-256 252 Indirect [INDIRECT] n - 253 Private
   * [PRIVATEDNS] y see below OPTIONAL 254 Private [PRIVATEOID] y see below OPTIONAL 255 reserved
   * 
   * 6 - 251 Available for assignment by IETF Standards Action.
   * 
   * see also: http://www.iana.org/assignments/dns-sec-alg-numbers/dns-sec-alg-numbers.xml
   * 
   */

  RSAMD5((byte) 1, "MD5withRSA"), DH((byte) 2, null), DSASHA1((byte) 3, "SHA1WithDSA"), ECC(
      (byte) 4,
      null), RSASHA1((byte) 5, "SHA1withRSA"), RSASHA1_NSEC3_SHA1((byte) 7, "SHA1withRSA"), // is
                                                                                            // alias
                                                                                            // for
                                                                                            // RSASHA1
  RSASHA256((byte) 8, "SHA256withRSA"), INDIRECT((byte) 252, null), PRIVATEDNS((byte) 253,
      null), PRIVATEOID((byte) 254, null);

  private byte value;
  private String algorithm;

  private AlgorithmType(byte value, String algorithm) {
    this.value = value;
    this.algorithm = algorithm;
  }

  private static Map<Integer, AlgorithmType> types = new HashMap<>();

  static {
    AlgorithmType[] values = values();
    for (AlgorithmType type : values) {
      types.put(Integer.valueOf(type.getValue()), type);
    }
  }

  public byte getValue() {
    return value;
  }

  public static AlgorithmType fromValue(short value) {
    return types.get(Integer.valueOf(value));
  }

  public String getAlgorithm() {
    return algorithm;
  }


}

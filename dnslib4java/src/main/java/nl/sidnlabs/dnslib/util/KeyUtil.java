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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import nl.sidnlabs.dnslib.message.records.dnssec.DNSKEYResourceRecord;
import nl.sidnlabs.dnslib.message.records.dnssec.DSResourceRecord;

public class KeyUtil {

  private KeyUtil() {}

  private static char KEY_ZONE_FLAG_MASK = 0x0100; // 0000 0001 0000 0000
  private static char KEY_ZONE_SEP_FLAG_MASK = 0x0101; // 0000 0001 0000 0001


  public static PublicKey createPublicKey(byte[] key, int algorithm) {
    if (algorithm == 5 || algorithm == 7 || algorithm == 8 || algorithm == 10) {

      // only create RSA pub keys.
      ByteBuffer b = ByteBuffer.wrap(key);

      int exponentLength = b.get() & 0xff;
      if (exponentLength == 0) {
        exponentLength = b.getChar();
      }
      try {
        byte[] data = new byte[exponentLength];
        b.get(data);
        BigInteger exponent = new BigInteger(1, data);
        byte[] modulusData = new byte[b.remaining()];
        b.get(modulusData);
        BigInteger modulus = new BigInteger(1, modulusData);

        KeyFactory factory = KeyFactory.getInstance("RSA");
        return factory.generatePublic(new RSAPublicKeySpec(modulus, exponent));
      } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
        // problem creating pub key
      }
    }

    // no support for Elliptic curves, Ghost etc
    return null;
  }


  /**
   * Bereken de keyTag(footprint) van een publieke sleutel. De keyTag berekent een getal waarmee de
   * publieke sleutel te herkennen is, dit is niet per definitie uniek per publieke sleutel. Zie
   * IETF RFC 4034, Appendix B voor meer informatie.
   * 
   * @see http://www.ietf.org/rfc/rfc4034.txt
   * 
   *      Dit lijkt op het berekenen van 1 complement checksum
   *      (http://nl.wikipedia.org/wiki/One%27s_complement) De onderstaande implementatie is
   *      overgenomen van versisign, zie:
   *      http://svn.verisignlabs.com/jdnssec/dnsjava/trunk/org/xbill/DNS/KEYBase.java
   * @param key een base64 encoded public key
   * @param algorimte, de naam van het algoritme waarmee de public key is gemaakt.
   * @return integer waarde welke de keytag van de public key is
   */
  public static int createKeyTag(byte[] rdata, int alg) {

    int foot = 0;
    int footprint = -1;

    // als de publieke sleuten met RSA/MD5 is gemaakt en gehashed dan
    // geld er een ander algoritme voor bepalen keytag

    if (1 == alg) { // MD5
      int d1 = rdata[rdata.length - 3] & 0xFF;
      int d2 = rdata[rdata.length - 2] & 0xFF;
      foot = (d1 << 8) + d2;
    } else {
      int i;
      for (i = 0; i < rdata.length - 1; i += 2) {
        int d1 = rdata[i] & 0xFF;
        int d2 = rdata[i + 1] & 0xFF;
        foot += ((d1 << 8) + d2);
      }
      if (i < rdata.length) {
        int d1 = rdata[i] & 0xFF;
        foot += (d1 << 8);
      }
      foot += ((foot >> 16) & 0xFFFF);
    }
    footprint = (foot & 0xFFFF);
    return footprint;
  }

  public static boolean isZoneKey(DNSKEYResourceRecord key) {
    return (key.getFlags() & KEY_ZONE_FLAG_MASK) == KEY_ZONE_FLAG_MASK;
  }

  public static boolean isSepKey(DNSKEYResourceRecord key) {
    return (key.getFlags() & KEY_ZONE_SEP_FLAG_MASK) == KEY_ZONE_SEP_FLAG_MASK;
  }

  public static boolean isKeyandDSmatch(DNSKEYResourceRecord key, DSResourceRecord ds) {
    return (key.getAlgorithm() == ds.getAlgorithm() && key.getKeytag() == ds.getKeytag()
        && key.getName().equalsIgnoreCase(ds.getName()));
  }

}

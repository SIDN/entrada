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
package nl.sidnlabs.dnslib.message.records.dnssec;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.apache.commons.codec.binary.Hex;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.records.AbstractResourceRecord;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.DigestType;

/**
 * The RDATA of the NSEC3PARAM RR is as shown below:
 * 
 * 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6
 * 7 8 9 0 1 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ | Hash Alg. | Flags |
 * Iterations | +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ | Salt Length |
 * Salt / +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * 
 * Hash Algorithm is a single octet.
 * 
 * Flags field is a single octet.
 * 
 * Iterations is represented as a 16-bit unsigned integer, with the most significant bit first.
 * 
 * Salt Length is represented as an unsigned octet. Salt Length represents the length of the
 * following Salt field in octets. If the value is zero, the Salt field is omitted.
 * 
 * Salt, if present, is encoded as a sequence of binary octets. The length of this field is
 * determined by the preceding Salt Length field.
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class NSEC3ParamResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;

  private DigestType hashAlgorithm;
  private short flags;
  // optout is the lsb of the flags octed
  private boolean optout;
  private char iterations;
  private short saltLength;
  private byte[] salt;

  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    hashAlgorithm = DigestType.fromValue(buffer.readUnsignedByte());

    flags = buffer.readUnsignedByte();

    optout = (flags & 0x01) == 0x01; // 0000 0001

    iterations = buffer.readUnsignedChar();

    saltLength = buffer.readUnsignedByte();

    if (saltLength > 0) {
      salt = new byte[saltLength];
      buffer.readBytes(salt);
    }


  }

  @Override
  public void encode(NetworkData buffer) {
    super.encode(buffer);

    buffer.writeChar(rdLength);

    buffer.writeBytes(rdata);
  }

  @Override
  public String toZone(int maxLength) {
    StringBuilder b = new StringBuilder();
    b.append(super.toZone(maxLength) + "\t" + hashAlgorithm.getValue() + " " + flags + " "
        + +(int) iterations + " ");

    if (saltLength == 0) {
      b.append("- ");
    } else {
      b.append(Hex.encodeHexString(salt) + " ");
    }

    return b.toString();
  }

  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = super.createJsonBuilder();
    return builder.add("rdata",
        Json.createObjectBuilder().add("hash-algorithm", hashAlgorithm.name()).add("flags", flags)
            .add("optout", optout).add("iterations", (int) iterations)
            .add("salt-length", saltLength).add("salt", Hex.encodeHexString(salt)))
        .build();
  }


}

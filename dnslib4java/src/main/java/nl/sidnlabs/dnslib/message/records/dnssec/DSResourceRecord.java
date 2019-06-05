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
import nl.sidnlabs.dnslib.types.AlgorithmType;
import nl.sidnlabs.dnslib.types.DigestType;

@Data
@EqualsAndHashCode(callSuper = true)
public class DSResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;


  /*
   * The RDATA for a DS RR consists of a 2 octet Key Tag field, a 1 octet Algorithm field, a 1 octet
   * Digest Type field, and a Digest field.
   * 
   * 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
   * 6 7 8 9 0 1 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ | Key Tag |
   * Algorithm | Digest Type | +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ / /
   * / Digest / / / +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   */

  private char keytag;
  private AlgorithmType algorithm;
  private DigestType digestType;
  private byte[] digest;

  private String hex;


  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    keytag = buffer.readUnsignedChar();

    short alg = buffer.readUnsignedByte();
    algorithm = AlgorithmType.fromValue(alg);

    short dt = buffer.readUnsignedByte();
    digestType = DigestType.fromValue(dt);

    digest = new byte[rdLength - 4];
    buffer.readBytes(digest);

    hex = new String(Hex.encodeHex(digest));
  }

  @Override
  public void encode(NetworkData buffer) {

    super.encode(buffer);

    buffer.writeChar(rdLength);

    buffer.writeChar(keytag);

    buffer.writeByte(algorithm.getValue());

    buffer.writeByte(digestType.getValue());

    buffer.writeBytes(digest);

  }

  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = super.createJsonBuilder();
    return builder.add("rdata",
        Json.createObjectBuilder().add("keytag", (int) keytag)
            .add("algorithm", algorithm != null ? algorithm.name() : "")
            .add("digest-type", digestType.name()).add("digest", hex))
        .build();
  }

}

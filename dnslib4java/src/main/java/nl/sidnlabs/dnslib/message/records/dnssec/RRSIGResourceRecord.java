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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.apache.commons.codec.binary.Base64;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.records.AbstractResourceRecord;
import nl.sidnlabs.dnslib.message.util.DNSStringUtil;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.AlgorithmType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.dnslib.types.TypeMap;
import nl.sidnlabs.dnslib.util.LabelUtil;

@Data
@EqualsAndHashCode(callSuper = true)
public class RRSIGResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;

  private static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("YYYYMMddHHmmss");


  /*
   * The RDATA for an RRSIG RR consists of a 2 octet Type Covered field, a 1 octet Algorithm field,
   * a 1 octet Labels field, a 4 octet Original TTL field, a 4 octet Signature Expiration field, a 4
   * octet Signature Inception field, a 2 octet Key tag, the Signer's Name field, and the Signature
   * field.
   * 
   * 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
   * 6 7 8 9 0 1 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ | Type Covered |
   * Algorithm | Labels | +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ |
   * Original TTL | +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ | Signature
   * Expiration | +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ | Signature
   * Inception | +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ | Key Tag | /
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ Signer's Name / / /
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ / / / Signature / / /
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   * 
   */


  private TypeMap typeCovered;
  private AlgorithmType algorithm;

  private short labels;
  private long originalTtl;
  private long signatureExpiration;
  private long signatureInception;
  private char keytag;
  private String signerName;
  private byte[] signature;
  private boolean wildcard;

  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    char type = buffer.readUnsignedChar();

    ResourceRecordType rrType = ResourceRecordType.fromValue(type);
    if (rrType == null) {
      rrType = ResourceRecordType.RESERVED;
    }

    typeCovered = new TypeMap(rrType, type);

    short alg = buffer.readUnsignedByte();
    algorithm = AlgorithmType.fromValue(alg);

    labels = buffer.readUnsignedByte();
    // check if wildacrd was used
    wildcard = LabelUtil.count(getName()) > labels;

    originalTtl = buffer.readUnsignedInt();

    signatureExpiration = buffer.readUnsignedInt();

    signatureInception = buffer.readUnsignedInt();

    keytag = buffer.readUnsignedChar();

    signerName = DNSStringUtil.readName(buffer);

    int signatureLength = rdLength;
    if (signerName.length() == 1) {
      // root
      signatureLength = signatureLength - 1;

    } else {
      // non root signer
      signatureLength = signatureLength - (signerName.length() + 1);
    }
    signatureLength = signatureLength - 18;

    signature = new byte[signatureLength];

    buffer.readBytes(signature);

    DATE_FMT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }



  @Override
  public void encode(NetworkData buffer) {

    super.encode(buffer);

    buffer.writeChar(rdLength);

    buffer.writeChar(typeCovered.getValue());

    buffer.writeByte(algorithm.getValue());

    buffer.writeByte(labels);

    buffer.writeInt((int) originalTtl);

    buffer.writeInt((int) signatureExpiration);

    buffer.writeInt((int) signatureInception);

    buffer.writeChar(keytag);

    DNSStringUtil.writeName(signerName, buffer);

    buffer.writeBytes(signature);

  }

  @Override
  public String toZone(int maxLength) {

    Date exp = new Date();
    exp.setTime((long) (signatureExpiration * 1000));

    Date incep = new Date();
    incep.setTime((long) (signatureInception * 1000));

    SimpleDateFormat fmt = new SimpleDateFormat("YYYYMMddHHmmss");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));

    return super.toZone(maxLength) + "\t" + typeCovered.name() + " " + algorithm.getValue() + " "
        + labels + " " + originalTtl + " " + fmt.format(exp) + "(\n\t\t\t\t\t" + fmt.format(incep)
        + " " + (int) keytag + " " + signerName + "\n\t\t\t\t\t"
        + new Base64(36, "\n\t\t\t\t\t".getBytes()).encodeAsString(signature) + " )";
  }


  @Override
  public JsonObject toJSon() {
    Date exp = new Date();
    exp.setTime((long) (signatureExpiration * 1000));

    Date incep = new Date();
    incep.setTime((long) (signatureInception * 1000));

    JsonObjectBuilder builder = super.createJsonBuilder();
    return builder
        .add("rdata",
            Json.createObjectBuilder().add("type-covered", typeCovered.name())
                .add("algorithm", algorithm.name()).add("labels", labels)
                .add("original-ttl", originalTtl).add("sig-exp", DATE_FMT.format(exp))
                .add("sig-inc", DATE_FMT.format(incep)).add("keytag", (int) keytag)
                .add("signer-name", signerName).add("signature",
                    new Base64(Integer.MAX_VALUE, "".getBytes()).encodeAsString(signature)))
        .build();
  }

  public boolean getWildcard() {
    return wildcard;
  }

}

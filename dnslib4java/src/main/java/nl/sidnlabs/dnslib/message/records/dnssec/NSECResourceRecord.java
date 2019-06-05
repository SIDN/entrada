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

import java.util.ArrayList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.records.AbstractResourceRecord;
import nl.sidnlabs.dnslib.message.util.DNSStringUtil;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.TypeMap;

@Data
@EqualsAndHashCode(callSuper = true)
public class NSECResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;

  /*
   * The RDATA of the NSEC RR is as shown below:
   * 
   * 1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
   * 6 7 8 9 0 1 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ / Next Domain
   * Name / +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+ / Type Bit Maps /
   * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   * 
   */

  private String nextDomainName;
  protected List<TypeMap> types = new ArrayList<>();


  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    nextDomainName = DNSStringUtil.readName(buffer);
    int octetAvailable = rdLength - (nextDomainName.length() + 1);
    new NSECTypeDecoder().decode(octetAvailable, buffer, types);
  }



  @Override
  public void encode(NetworkData buffer) {
    super.encode(buffer);

    buffer.writeChar(rdLength);

    // DNSStringUtil.writeName(nextDomainName, buffer);

    buffer.writeBytes(getRdata());
  }

  @Override
  public String toString() {
    return "NSECResourceRecord [rdLength=" + (int) rdLength + ", nextDomainName=" + nextDomainName
        + "]";
  }


  @Override
  public String toZone(int maxLength) {
    StringBuilder b = new StringBuilder();
    b.append(super.toZone(maxLength) + "\t" + nextDomainName + " ");

    for (TypeMap type : types) {
      b.append(type.name() + " ");
    }

    return b.toString();
  }

  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = super.createJsonBuilder();
    builder.add("rdata", Json.createObjectBuilder().add("next-domainname", nextDomainName));

    JsonArrayBuilder typeBuilder = Json.createArrayBuilder();
    for (TypeMap type : types) {
      typeBuilder.add(type.getType().name());
    }
    return builder.add("types", typeBuilder.build()).build();
  }

}

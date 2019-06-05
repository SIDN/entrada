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
package nl.sidnlabs.dnslib.message.records;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.util.DNSStringUtil;
import nl.sidnlabs.dnslib.message.util.NetworkData;

@Data
@EqualsAndHashCode(callSuper = true)
public class SRVResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;

  private char priority;
  private char weight;
  private char port;
  private String target;


  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    priority = buffer.readUnsignedChar();

    weight = buffer.readUnsignedChar();

    port = buffer.readUnsignedChar();

    target = DNSStringUtil.readName(buffer);
  }

  @Override
  public void encode(NetworkData buffer) {
    super.encode(buffer);

    buffer.writeChar(6 + target.length() + 2);

    buffer.writeChar(priority);

    buffer.writeChar(weight);

    buffer.writeChar(port);

    DNSStringUtil.writeName(target, buffer);
  }

  @Override
  public String toZone(int maxLength) {
    return super.toZone(maxLength) + "\t" + (int) priority + " " + (int) weight + " " + (int) port
        + " " + target;
  }


  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = super.createJsonBuilder();
    return builder.add("rdata", Json.createObjectBuilder().add("priority", (int) priority))
        .add("weight", (int) weight).add("port", (int) port).add("target", target).build();
  }

}

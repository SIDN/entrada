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
import nl.sidnlabs.dnslib.message.util.NetworkData;

@Data
@EqualsAndHashCode(callSuper = true)
public class URIResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1023178649874976277L;
  private int priority;
  private int weight;
  private String uri;


  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);
    priority = buffer.readUnsignedChar();
    weight = buffer.readUnsignedChar();
    byte[] uriBytes = new byte[rdLength - 4];
    buffer.readBytes(uriBytes);
    uri = new String(uriBytes);
  }

  @Override
  public void encode(NetworkData buffer) {
    super.encode(buffer);

    // write rdlength
    buffer.writeChar(rdLength);
    buffer.writeChar(priority);
    buffer.writeChar(weight);
    buffer.writeBytes(uri.getBytes());
  }

  @Override
  public String toZone(int maxLength) {
    return super.toZone(maxLength) + "\t" + priority + " " + weight + " " + uri;
  }

  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = super.createJsonBuilder();
    return builder
        .add("rdata",
            Json
                .createObjectBuilder()
                .add("priority", priority)
                .add("weight", weight)
                .add("uri", uri))
        .build();
  }



}

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
public class NAPTRResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 6981260851592320670L;
  private char order;
  private char preference;
  private String flags;
  private String services;
  private String regexp;
  private String replacement;

  private int length;

  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    order = buffer.readUnsignedChar();

    preference = buffer.readUnsignedChar();

    flags = DNSStringUtil.readLabelData(buffer);
    length = 5 + flags.length();

    services = DNSStringUtil.readLabelData(buffer);
    length = length + services.length() + 1; // 3x16 bits + 1byte services length

    regexp = DNSStringUtil.readLabelData(buffer);
    length = length + regexp.length() + 1;

    replacement = DNSStringUtil.readName(buffer);
    if (replacement == null || replacement.length() == 0) {
      length = length + 1; // zero byte only
    } else {
      length = length + replacement.length() + 1;
    }

  }


  @Override
  public void encode(NetworkData buffer) {
    super.encode(buffer);

    buffer.writeChar(length);

    buffer.writeChar(preference);

    DNSStringUtil.writeLabelData(flags, buffer);

    DNSStringUtil.writeLabelData(services, buffer);

    DNSStringUtil.writeLabelData(regexp, buffer);

    DNSStringUtil.writeName(replacement, buffer);
  }

  @Override
  public String toZone(int maxLength) {
    return super.toZone(maxLength) + "\t" + (int) order + " " + preference + " \"" + flags + "\" "
        + "\"" + services + "\" " + "\"" + regexp + "\" " + replacement;
  }


  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = super.createJsonBuilder();
    return builder.add("rdata",
        Json.createObjectBuilder().add("order", (int) order).add("preference", (int) preference)
            .add("flags", flags).add("services", services).add("regexp", regexp)
            .add("replacement", replacement).add("length", length))
        .build();
  }

}

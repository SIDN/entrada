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

import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import com.google.common.net.InetAddresses;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.util.NetworkData;

@Data
@EqualsAndHashCode(callSuper = true)
public class AAAAResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;
  /*
   * 
   * A 128 bit IPv6 address is encoded in the data portion of an AAAA resource record in network
   * byte order (high-order byte first).
   * 
   */

  private String address;
  private byte[] ipv6Bytes;

  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    if (rdLength == 16) {
      // get the 16 raw bytes for the ipv6 address
      ipv6Bytes = buffer.readBytes(16, 16);

      // create a textual representation of the address
      InetAddress ipv6Addres;
      try {
        ipv6Addres = InetAddress.getByAddress(ipv6Bytes);
      } catch (UnknownHostException e) {
        throw new RuntimeException("Illegal ipv6 address", e);
      }

      setAddress(InetAddresses.toAddrString(ipv6Addres));
    }
  }

  @Override
  public void encode(NetworkData buffer) {
    super.encode(buffer);

    // write rdlength
    buffer.writeChar(rdLength);

    buffer.writeBytes(ipv6Bytes);
  }

  @Override
  public String toZone(int maxLength) {
    return super.toZone(maxLength) + "\t" + address;
  }

  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = super.createJsonBuilder();
    return builder.add("rdata", Json.createObjectBuilder().add("address", address)).build();
  }

}

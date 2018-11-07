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
package nl.sidn.dnslib.message.records;

import java.net.InetAddress;
import java.net.UnknownHostException;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import nl.sidn.dnslib.message.util.NetworkData;

public class AResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;

  private String address;
  private int[] ipv4Bytes;

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }



  @Override
  public void decode(NetworkData buffer) {
    super.decode(buffer);

    if (rdLength == 4) {
      InetAddress ip;
      byte[] addrBytes = buffer.readBytes(4, 4);
      try {
        ip = InetAddress.getByAddress(addrBytes);
      } catch (UnknownHostException e) {
        throw new RuntimeException("Invalid IP address", e);
      }
      setAddress(ip.getHostAddress());
    }
  }

  @Override
  public void encode(NetworkData buffer) {
    super.encode(buffer);

    // write rdlength
    buffer.writeChar(rdLength);
    if (ipv4Bytes != null && ipv4Bytes.length == 4) {
      for (int i = 0; i < 4; i++) {
        buffer.writeByte(ipv4Bytes[i]);
      }
    }
  }

  public String getCacheId() {
    return address;
  }

  @Override
  public String toString() {
    return super.toString() + " AResourceRecord [address=" + address + "]";
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

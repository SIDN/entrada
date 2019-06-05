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
package nl.sidnlabs.dnslib.message.records.edns0;

import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.exception.DnsDecodeException;
import nl.sidnlabs.dnslib.message.util.NetworkData;

/**
 * http://tools.ietf.org/html/draft-vandergaast-edns-client-subnet-02
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ClientSubnetOption extends EDNS0Option {

  private int fam;
  private int sourcenetmask;
  private int scopenetmask;
  private String address;
  private InetAddress inetAddress;

  public ClientSubnetOption() {}

  public ClientSubnetOption(int code, int len, NetworkData buffer) {
    super(code, len, buffer);
  }

  public String export() {
    return (fam == 1 ? "4," : "6,") + address + "/" + sourcenetmask + "," + scopenetmask;
  }

  public boolean isIPv4() {
    return fam == 1;
  }

  @Override
  public void decode(NetworkData buffer) {
    fam = buffer.readUnsignedChar();
    sourcenetmask = buffer.readUnsignedByte();
    scopenetmask = buffer.readUnsignedByte();
    int addrLength = len - 4; // -4 byte offset for fam+source+scope

    if (addrLength > 0) {

      byte[] addrBytes = new byte[0];

      // read available ip bytes, can be less than the bytes
      // required for a full ip addr.
      if (fam == 1) { // IP v4, 4 bytes
        // the ipv4 bytes can be < than the req 4 bytes.
        addrBytes = buffer.readBytes(4, Math.min(4, addrLength));
      } else if (fam == 2) { // v6 16 bytes
        // the ipv6 bytes can be < than the req 16 bytes.
        addrBytes = buffer.readBytes(16, Math.min(16, addrLength));
      }

      try {
        inetAddress = InetAddress.getByAddress(addrBytes);
      } catch (UnknownHostException e) {
        throw new DnsDecodeException("Invalid IP address", e);
      }
      address = inetAddress.getHostAddress();
    }
  }

}

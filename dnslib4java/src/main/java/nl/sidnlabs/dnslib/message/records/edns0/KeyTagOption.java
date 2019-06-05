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

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.util.NetworkData;

/**
 * @see https://tools.ietf.org/html/rfc8145#section-4.1
 * 
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class KeyTagOption extends EDNS0Option {

  private List<Integer> keytags;

  public KeyTagOption() {}

  public KeyTagOption(int code, int len, NetworkData buffer) {
    super(code, len, buffer);
  }

  @Override
  public void decode(NetworkData buffer) {
    keytags = new ArrayList<>();
    // get the actual padding data, to move pointer to end of packet.
    // ignore data read.
    if (len > 0) {
      if (len % 2 == 0) {
        // only read keytags if correct even number of bytes found
        int keys = len / 2;
        for (int i = 0; i < keys; i++) {
          // read 2 bytes
          keytags.add((int) buffer.readUnsignedChar());
        }
      } else {
        // illegal optionlen size, read data to get pointer in correct loc, ignore data.
        byte[] data = new byte[len];
        buffer.readBytes(data);
      }
    }
  }

}

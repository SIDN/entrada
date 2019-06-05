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

import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.util.NetworkData;


/**
 * @see http://tools.ietf.org/html/draft-hubert-ulevitch-edns-ping-01
 * 
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class PingOption extends EDNS0Option {

  private byte[] ping;

  public PingOption() {}

  public PingOption(int code, int len, NetworkData opt) {
    super(code, len, opt);
  }

  @Override
  public void decode(NetworkData buffer) {
    ping = new byte[4];
    buffer.readBytes(ping);
  }
}

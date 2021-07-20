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
package nl.sidnlabs.entrada.support;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.pcap.packet.Packet;

@Getter
@Setter
@AllArgsConstructor
public class RowData {

  public static final RowData NULL = new RowData(null, null, null, null, false);

  private Packet request;
  private Message requestMessage;
  private Packet response;
  private Message responseMessage;
  // true if this packet has expired from cache
  private boolean expired;

}

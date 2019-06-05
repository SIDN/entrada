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
import lombok.Data;
import nl.sidn.dnslib.message.Message;
import nl.sidn.pcap.packet.Packet;
import nl.sidnlabs.entrada.config.ServerInfo;

@Data
@AllArgsConstructor
public class PacketCombination {

  public static final PacketCombination NULL =
      new PacketCombination(null, null, null, null, null, false, null);

  private Packet request;
  private Message requestMessage;
  private ServerInfo server;
  private Packet response;
  private Message responseMessage;
  // true if this packet has expired from cache
  private boolean expired;
  private String pcapFilename;

}

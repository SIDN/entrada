/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with ENTRADA. If
 * not, see [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidnlabs.pcap.packet;

import com.google.common.collect.ComparisonChain;
import lombok.Data;

@Data
public class TCPFlow implements Comparable<TCPFlow> {
  private String src;
  private int srcPort;
  private String dst;
  private int dstPort;
  private short protocol;

  public TCPFlow() {}

  public TCPFlow(String src, int srcPort, String dst, int dstPort, short protocol) {
    this.src = src;
    this.srcPort = srcPort;
    this.dst = dst;
    this.dstPort = dstPort;
    this.protocol = protocol;
  }

  @Override
  public int compareTo(TCPFlow o) {
    return ComparisonChain.start().compare(src, o.src).compare(srcPort, o.srcPort)
        .compare(dst, o.dst).compare(dstPort, o.dstPort).compare(protocol, o.protocol).result();
  }

}

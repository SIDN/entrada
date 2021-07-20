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
package nl.sidnlabs.entrada.load.stream;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

/**
 * Output writer that will write output using separate thread. For now this class only supports
 * Parquet output
 *
 */
@Log4j2
@Component
// use prototype scope, create new bean each time batch of files is processed
// this to avoid problems with locking when builder is executed using multipel threads
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RowBuilderOperator {

  // // metric counters
  // private int rowCounter;
  // private int dnsCounter;
  // private int icmpCounter;
  //
  // private RowBuilder dnsRowBuilder;
  // private RowBuilder icmpRowBuilder;
  //
  // public RowBuilderOperator(ServerContext serverCtx, @Qualifier("dns") RowBuilder dnsRowBuilder,
  // @Qualifier("icmp") RowBuilder icmpRowBuilder, MeterRegistry registry) {
  //
  // this.dnsRowBuilder = dnsRowBuilder;
  // this.icmpRowBuilder = icmpRowBuilder;
  // }
  //
  // /**
  // * Lookup protocol, if no request is found then get the proto from the response.
  // *
  // * @param p
  // * @return
  // */
  // private int lookupProtocol(RowData p) {
  // if (p.getRequest() != null) {
  // return p.getRequest().getProtocol();
  // } else if (p.getResponse() != null) {
  // return p.getResponse().getProtocol();
  // }
  //
  // // unknown proto
  // return -1;
  // }
  //
  // public Pair<Row, List> process(RowData p, String svr) {
  // if (p != null) {
  // rowCounter++;
  // int proto = lookupProtocol(p);
  // if (proto == PacketFactory.PROTOCOL_TCP || proto == PacketFactory.PROTOCOL_UDP) {
  // dnsCounter++;
  // return dnsRowBuilder.build(p, svr);
  // } else if (proto == PacketFactory.PROTOCOL_ICMP_V4
  // || proto == PacketFactory.PROTOCOL_ICMP_V6) {
  // icmpCounter++;
  // return icmpRowBuilder.build(p, svr);
  // }
  // }
  //
  // return null;
  // }
  //
  // public void printStats() {
  // log.info("------------------ Row Builder stats ---------------------");
  // log.info("Rows: {}", rowCounter);
  // log.info("DNS: {}", dnsCounter);
  // log.info("ICMP: {}", icmpCounter);
  // }
  //
  // public void reset() {
  // rowCounter = 0;
  // dnsCounter = 0;
  // icmpCounter = 0;
  // }


}

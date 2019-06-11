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
package nl.sidnlabs.entrada.parquet;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.config.Settings;
import nl.sidnlabs.entrada.load.OutputHandler;
import nl.sidnlabs.entrada.metric.MetricManager;
import nl.sidnlabs.entrada.model.Row;
import nl.sidnlabs.entrada.model.RowBuilder;
import nl.sidnlabs.entrada.support.PacketCombination;
import nl.sidnlabs.entrada.support.RequestKey;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.decoder.ICMPDecoder;
import nl.sidnlabs.pcap.packet.Packet;

/**
 * Output handler for Apache Parquet format.
 * 
 * @author maartenwullink
 *
 */
@Log4j2
@Component
public class ParquetOutputHandler implements OutputHandler {

  protected final Map<RequestKey, Packet> requestCache = new HashMap<>();

  // default max 3 million packets per files max (+/- 125mb files)
  @Value("${entrada.parquet.max:3000000}")
  private int parquetMaxPackets;

  @Value("${entrada.location.output}")
  private String outputLocation;

  // metric counters
  private int counter;
  private int totalCounter;
  private int icmpCounter;
  private int icmpTotalCounter;

  private DNSParquetPacketWriter dnsWriter;
  private ICMPParquetPacketWriter icmpWriter;
  private MetricManager metricManager;
  private RowBuilder dnsRowBuilder;
  private RowBuilder icmpRowBuilder;



  public ParquetOutputHandler(DNSParquetPacketWriter dnsWriter, ICMPParquetPacketWriter icmpWriter,
      MetricManager metricManager, @Qualifier("dnsBuilder") RowBuilder dnsRowBuilder,
      @Qualifier("icmpBuilder") RowBuilder icmpRowBuilder) {

    this.dnsWriter = dnsWriter;
    this.icmpWriter = icmpWriter;
    this.metricManager = metricManager;
    this.dnsRowBuilder = dnsRowBuilder;
    this.icmpRowBuilder = icmpRowBuilder;
  }


  @Override
  public void handle(PacketCombination p) {
    if (p != null && p != PacketCombination.NULL) {

      int proto = lookupProtocol(p);
      if (proto == PcapReader.PROTOCOL_TCP) {
        writeDns(dnsRowBuilder.build(p), p.getServer().getName());
      } else if (proto == PcapReader.PROTOCOL_UDP) {
        writeDns(dnsRowBuilder.build(p), p.getServer().getName());
      } else if (proto == ICMPDecoder.PROTOCOL_ICMP_V4 || proto == ICMPDecoder.PROTOCOL_ICMP_V6) {
        writeIcmp(icmpRowBuilder.build(p), p.getServer().getName());
      }
    } else {
      log.info("processed " + totalCounter + " DNS packets.");
      log.info("processed " + icmpTotalCounter + " ICMP packets.");

      metricManager.send(MetricManager.METRIC_IMPORT_DNS_COUNT, totalCounter);
      metricManager.send(MetricManager.METRIC_ICMP_COUNT, icmpTotalCounter);
      dnsRowBuilder.writeMetrics();
      icmpRowBuilder.writeMetrics();
    }
  }

  /**
   * Lookup protocol, if no request is found then get the proto from the response.
   * 
   * @param p
   * @return
   */
  private int lookupProtocol(PacketCombination p) {
    if (p.getRequest() != null) {
      return p.getRequest().getProtocol();
    } else if (p.getResponse() != null) {
      return p.getResponse().getProtocol();
    }

    // unknown proto
    return -1;
  }

  private void writeDns(Row row, String server) {
    dnsWriter.write(row, server);
    counter++;
    totalCounter++;
    if (counter >= parquetMaxPackets) {
      log.info("Max DNS packets reached for this Parquet file, close current file and create new");

      dnsWriter.close();
      // create new writer
      dnsWriter.open(outputLocation, Settings.getServerInfo().getFullname(), "dnsdata");
      // reset counter
      counter = 0;
    }
  }

  private void writeIcmp(Row row, String server) {
    icmpWriter.write(row, server);
    icmpCounter++;
    icmpTotalCounter++;
    if (icmpCounter >= parquetMaxPackets) {
      log.info("Max ICMP packets reached for this Parquet file, close current file and create new");

      icmpWriter.close();
      // create new writer
      icmpWriter.open(outputLocation, Settings.getServerInfo().getFullname(), "icmpdata");
      // reset counter
      icmpCounter = 0;
    }
  }

  public void close() {
    // make sure to close last partial file
    dnsWriter.close();
    icmpWriter.close();
  }

  public void open(boolean dns, boolean icmp) {
    if (dns) {
      dnsWriter.open(outputLocation, Settings.getServerInfo().getFullname(), "dnsdata");
    }

    if (icmp) {
      icmpWriter.open(outputLocation, Settings.getServerInfo().getFullname(), "icmpdata");
    }
  }

}

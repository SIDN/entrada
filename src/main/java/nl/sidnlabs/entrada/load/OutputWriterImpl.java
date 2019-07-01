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
package nl.sidnlabs.entrada.load;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.Row;
import nl.sidnlabs.entrada.model.RowBuilder;
import nl.sidnlabs.entrada.support.RequestCacheKey;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;

/**
 * Output writer that will write output using separate thread. For now this class only supports
 * Parquet output
 *
 */
@Log4j2
@Component
public class OutputWriterImpl implements OutputWriter {

  private static final int ROW_BATCH_SIZE = 10000;

  protected final Map<RequestCacheKey, Packet> requestCache = new HashMap<>();

  @Value("${entrada.location.work}")
  private String workLocation;

  // metric counters
  private Counter dnsCounter;
  private Counter icmpCounter;

  private boolean open;
  private RowWriter dnsWriter;
  private RowWriter icmpWriter;
  private RowBuilder dnsRowBuilder;
  private RowBuilder icmpRowBuilder;
  private ServerContext serverCtx;

  private Set<Partition> dnsPartitions = new HashSet<>();
  private Set<Partition> icmpPartitions = new HashSet<>();

  public OutputWriterImpl(ServerContext serverCtx, @Qualifier("parquet-dns") RowWriter dnsWriter,
      @Qualifier("parquet-icmp") RowWriter icmpWriter,
      @Qualifier("dnsBuilder") RowBuilder dnsRowBuilder,
      @Qualifier("icmpBuilder") RowBuilder icmpRowBuilder, MeterRegistry registry) {

    this.serverCtx = serverCtx;
    // when multiple formats are support we need a factory to create the output writers for the
    // correct format
    this.dnsWriter = dnsWriter;
    this.icmpWriter = icmpWriter;
    this.dnsRowBuilder = dnsRowBuilder;
    this.icmpRowBuilder = icmpRowBuilder;

    dnsCounter = registry.counter("packets.dns");
    icmpCounter = registry.counter("packets.icmp");
  }


  private void write(RowData p) {
    if (p != null) {
      int proto = lookupProtocol(p);
      if (proto == PacketFactory.PROTOCOL_TCP) {
        writeDns(dnsRowBuilder.build(p), serverCtx.getServerInfo().getName());
      } else if (proto == PacketFactory.PROTOCOL_UDP) {
        writeDns(dnsRowBuilder.build(p), serverCtx.getServerInfo().getName());
      } else if (proto == PacketFactory.PROTOCOL_ICMP_V4
          || proto == PacketFactory.PROTOCOL_ICMP_V6) {
        writeIcmp(icmpRowBuilder.build(p), serverCtx.getServerInfo().getName());
      }
    }
  }

  /**
   * Lookup protocol, if no request is found then get the proto from the response.
   * 
   * @param p
   * @return
   */
  private int lookupProtocol(RowData p) {
    if (p.getRequest() != null) {
      return p.getRequest().getProtocol();
    } else if (p.getResponse() != null) {
      return p.getResponse().getProtocol();
    }

    // unknown proto
    return -1;
  }

  private void writeDns(Row row, String server) {
    dnsPartitions.add(dnsWriter.write(row, server));
    dnsCounter.increment();
  }

  private void writeIcmp(Row row, String server) {
    icmpPartitions.add(icmpWriter.write(row, server));
    icmpCounter.increment();
  }

  private Map<String, Set<Partition>> close() {
    log.info("Closing writer");

    // make sure to close last partial file
    dnsWriter.close();
    icmpWriter.close();

    open = false;
    // copy partition set and then clear the set
    Map<String, Set<Partition>> results = new HashMap<>();
    results.put("dns", new HashSet<>(dnsPartitions));
    results.put("icmp", new HashSet<>(icmpPartitions));

    dnsPartitions.clear();
    icmpPartitions.clear();
    // delete temp files and metadata generated by parquet lib.
    return results;
  }

  private void open(boolean dns, boolean icmp) {
    log.info("Opening writer");

    open = true;
    if (dns) {
      dnsWriter.open(workLocation, serverCtx.getServerInfo().getNormalizeName(), "dns");
    }

    if (icmp) {
      icmpWriter.open(workLocation, serverCtx.getServerInfo().getNormalizeName(), "icmp");
    }
  }

  public boolean isOpen() {
    return open;
  }

  /**
   * Write row to disk
   * 
   * @param batch list of rows
   * @return true if the final PacketCombination.NULL packet has been received.
   */
  private boolean process(List<RowData> batch) {
    for (RowData pc : batch) {
      if (pc == RowData.NULL) {
        // stop
        return true;
      }
      write(pc);
    }
    return false;
  }

  /**
   * Start reading from the input queue until the PacketCombination.NULL has been received. This
   * method uses @Async so it will be executed on a separate thread.
   */
  @Override
  @Async
  public Future<Map<String, Set<Partition>>> start(boolean dns, boolean icmp,
      LinkedBlockingQueue<RowData> input) {

    try {
      log.info("Open writer");
      open(dns, icmp);
      // read data from queue
      read(input);
    } catch (Exception e) {
      log.error("Writer thread exception", e);
    }

    // return future with the created partitions
    return new AsyncResult<>(close());
  }

  private void read(LinkedBlockingQueue<RowData> input) {
    List<RowData> batch = new ArrayList<>();
    while (true) {
      batch.clear();
      if (input.drainTo(batch, ROW_BATCH_SIZE) > 0) {
        if (process(batch)) {
          log.info("Received final packet, close output");
          log.info("processed " + (int) dnsCounter.count() + " DNS packets.");
          log.info("processed " + (int) icmpCounter.count() + " ICMP packets.");

          return;
        }
      } else {
        // no data from queue, sleep for a while
        sleep();
      }
    }
  }


  private void sleep() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      // ignore this error
      log.error("Thread interrupted during sleep");
    }
  }
}

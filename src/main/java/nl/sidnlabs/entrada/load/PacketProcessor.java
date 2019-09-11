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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.service.ArchiveService;
import nl.sidnlabs.entrada.service.PartitionService;
import nl.sidnlabs.entrada.service.UploadService;
import nl.sidnlabs.entrada.support.RequestCacheKey;
import nl.sidnlabs.entrada.support.RequestCacheValue;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.entrada.util.CompressionUtil;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Datagram;
import nl.sidnlabs.pcap.packet.DatagramPayload;
import nl.sidnlabs.pcap.packet.FlowData;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;
import nl.sidnlabs.pcap.packet.TCPFlow;

@Log4j2
@Component
// use prototype scope, create new bean each time batch of files is processed
// this to avoid problems with memory/caches when running app for a long period of time
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PacketProcessor {

  private static final int MAX_QUEUE_SIZE = 100000;

  @Value("${entrada.cache.timeout}")
  private int cacheTimeoutConfig;

  @Value("${entrada.cache.timeout.tcp.flows}")
  private int cacheTimeoutTCPConfig;

  @Value("${entrada.cache.timeout.ip.fragmented}")
  private int cacheTimeoutIPFragConfig;

  @Value("${entrada.inputstream.buffer:64}")
  private int bufferSizeConfig;

  @Value("${entrada.icmp.enable}")
  private boolean icmpEnabled;

  @Value("${entrada.location.input}")
  private String inputLocation;

  @Value("${entrada.input.file.skipfirst}")
  private boolean skipFirst;

  @Value("${management.metrics.export.graphite.enabled}")
  private boolean metrics;

  @Value("#{${entrada.partition.activity.ping:5}*60*1000}")
  private int maxPartitionPingMs;

  private PcapReader pcapReader;
  protected Map<RequestCacheKey, RequestCacheValue> requestCache;

  private StateManager stateManager;
  private OutputWriter outputWriter;
  private ArchiveService fileArchiveService;

  // max lifetime for cached packets, in milliseconds (configured in minutes)
  private int cacheTimeout;
  private int packetCounter;

  // keep list of active zone transfers
  private Map<RequestCacheKey, Integer> activeZoneTransfers;
  private FileManagerFactory fileManagerFactory;
  private PartitionService partitionService;
  private ServerContext serverCtx;
  private Future<Map<String, Set<Partition>>> outputFuture;
  private LinkedBlockingQueue<RowData> rowQueue;
  private HistoricalMetricManager metricManager;
  private UploadService uploadService;
  // maps with state, loaded at start and persisted at end
  private Map<TCPFlow, FlowData> flows = new HashMap<>();
  private Multimap<Datagram, DatagramPayload> datagrams = TreeMultimap.create();

  public PacketProcessor(ServerContext serverCtx, StateManager persistenceManager,
      OutputWriter outputWriter, ArchiveService fileArchiveService,
      FileManagerFactory fileManagerFactory, PartitionService partitionService,
      HistoricalMetricManager historicalMetricManager, UploadService uploadService) {

    this.serverCtx = serverCtx;
    this.stateManager = persistenceManager;
    this.outputWriter = outputWriter;
    this.fileArchiveService = fileArchiveService;
    this.fileManagerFactory = fileManagerFactory;
    this.partitionService = partitionService;
    this.uploadService = uploadService;
    // convert minutes to milliseconds
    this.cacheTimeout = 1000 * 60 * cacheTimeoutConfig;
    this.metricManager = historicalMetricManager;
  }

  public void execute() {
    // reset all counters and reused data structures
    reset();

    // search for input files
    List<String> inputFiles = scan();
    if (inputFiles.isEmpty()) {
      // no files found to process, stop
      return;
    }

    long procStart = System.currentTimeMillis();

    // get the state from the previous run
    loadState();
    int fileCounter = 0;
    for (String file : inputFiles) {
      Date start = new Date();

      if (fileArchiveService.exists(file, serverCtx.getServerInfo().getName())) {
        if (log.isDebugEnabled()) {
          log.info("file {} already processed!, continue with next file", file);
        }
        // move the pcap file to archive location or delete
        fileArchiveService.archive(file, start, 0);
        continue;
      }

      fileCounter++;
      if (outputFuture == null) {
        // open the output file writer
        outputFuture = outputWriter.start(true, icmpEnabled, rowQueue);
      }
      read(file);
      // flush expired packets after every file, to avoid a large cache eating up the heap
      purgeCache();
      // move the pcap file to archive location or delete
      fileArchiveService.archive(file, start, (int) packetCounter);
      // make sure the active paritions are not compacted during bulk loading
      if (pingPartitions(procStart)) {
        // reset timer
        procStart = System.currentTimeMillis();
      }

      logStats();
    }

    // check if any file have been processed if so, send "end" packet to writer and wait foor writer
    // to finish
    if (Objects.nonNull(outputFuture)) {
      // mark all data procssed
      pushRow(RowData.NULL);
      // wait until writer is done
      log.info("Wait until output writer(s) have finished");
      Map<String, Set<Partition>> partitions = waitForWriter(outputFuture);
      log.info("Output writer(s) have finished, continue with uploading the output data");
      // upload newly created data to fs
      uploadService.upload(partitions);
      createPartitions(partitions);
      // save unmatched packet state to file
      // the next pcap might have the missing responses
      persistState();
    }

    log.info("Ready, processed {} new files", fileCounter);
  }

  /**
   * Update the last updated date for active partitions, to prevent the partition from being
   * compacted when processing a bulk load of pcap data which might take hours and in this time the
   * partition is otherwise not updated in the database.
   * 
   * @param procStart
   * @return
   */
  private boolean pingPartitions(long start) {
    long now = System.currentTimeMillis();

    if ((now - start) > maxPartitionPingMs) {
      // last partition ping was > configured for partitionActivePing
      outputWriter
          .activePartitions()
          .entrySet()
          .stream()
          .forEach(e -> partitionService.ping(e.getKey(), e.getValue()));

      return true;
    }

    return false;
  }

  private void createPartitions(Map<String, Set<Partition>> partitions) {
    partitions.entrySet().stream().forEach(e -> partitionService.create(e.getKey(), e.getValue()));
  }

  private Map<String, Set<Partition>> waitForWriter(
      Future<Map<String, Set<Partition>>> outputFuture) {

    try {
      return outputFuture.get();
    } catch (Exception e) {
      // ignore this error, return empty set
      log.error("Error while waiting for output writer to finish", e);
    }

    return Collections.emptyMap();
  }

  private void reset() {
    packetCounter = 0;
    requestCache = new HashMap<>();
    activeZoneTransfers = new HashMap<>();
    outputFuture = null;
    rowQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
  }

  private void logStats() {
    log.info("--------- Done processing data-----------");
    log.info("{} packets", packetCounter);
    log.info("-----------------------------------------");
  }

  private void read(String file) {
    log.info("Start reading from file {}", file);

    // try to open file, if file is not good pcap handle exception and fail fast.
    if (!createReader(file)) {
      log.error("Skip bad input file: " + file);
      return;
    }

    long readStart = System.currentTimeMillis();
    // get filename only to map parquet row to pcap file for possible
    // later detailed analysis
    String fileName = FileUtil.filename(file);

    // process each packet from the pcap file
    try {
      pcapReader.stream().forEach(p -> process(p, fileName));

      log.info("Processing time: " + (System.currentTimeMillis() - readStart) + "ms");
      if (log.isDebugEnabled()) {
        log.debug("Done with decoding, start cleanup");
      }
    } finally {
      // clear expired cache entries
      pcapReader
          .clearCache(cacheTimeoutTCPConfig * 60 * 1000, cacheTimeoutIPFragConfig * 60 * 1000);
      // make sure the pcap reader is always closed to avoid leaks
      pcapReader.close();
    }

  }

  private boolean isICMP(Packet p) {
    return p.getProtocol() == PacketFactory.PROTOCOL_ICMP_V4
        || p.getProtocol() == PacketFactory.PROTOCOL_ICMP_V6;
  }

  private void process(Packet currentPacket, String fileName) {
    packetCounter++;
    if (packetCounter % 100000 == 0) {
      log.info("Processed " + packetCounter + " packets");
    }

    if (isICMP(currentPacket)) {
      if (!icmpEnabled) {
        // do not process ICMP packets
        return;
      }
      // handle icmp
      pushRow(new RowData(currentPacket, null, null, null, false, fileName));

    } else {
      // must be dnspacket
      DNSPacket dnsPacket = (DNSPacket) currentPacket;
      if (dnsPacket.getMessages().isEmpty()) {
        // skip malformed packets
        log.debug("Packet contains no dns message, skipping...");
        return;
      }

      for (Message msg : dnsPacket.getMessages()) {
        // put request into map until we find matching response, with a key based on: query id,
        // qname, ip src, tcp/udp port add time for possible timeout eviction
        if (msg.getHeader().getQr() == MessageType.QUERY) {
          handDnsQuery(dnsPacket, msg, fileName);
        } else {
          handDnsResponse(dnsPacket, msg, fileName);
        }
      }
      // clear the packet which may contain many dns messages
      dnsPacket.clear();
    } // end of dns packet
  }

  /**
   * get qname from request which is part of the cache lookup key
   * 
   * @param msg the DNS message
   * @return the qname from the DNS question or null if not found.
   */
  private String qname(Message msg) {
    String qname = null;
    if (!msg.getQuestions().isEmpty()) {
      qname = msg.getQuestions().get(0).getQName();
    }

    return qname;
  }

  private void handDnsQuery(DNSPacket dnsPacket, Message msg, String fileName) {
    // check for ixfr/axfr request
    if (!msg.getQuestions().isEmpty()
        && (msg.getQuestions().get(0).getQType() == ResourceRecordType.AXFR
            || msg.getQuestions().get(0).getQType() == ResourceRecordType.IXFR)) {

      if (log.isDebugEnabled()) {
        log.debug("Detected zone transfer for: " + dnsPacket.getFlow());
      }
      // keep track of ongoing zone transfer, we do not want to store all the response
      // packets for an ixfr/axfr.
      activeZoneTransfers
          .put(new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getSrc(),
              dnsPacket.getSrcPort()), 0);
    }

    RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), qname(msg),
        dnsPacket.getSrc(), dnsPacket.getSrcPort(), System.currentTimeMillis());

    // put the query in the cache until we get a matching response
    requestCache.put(key, new RequestCacheValue(msg, dnsPacket, fileName));
  }

  private void handDnsResponse(DNSPacket dnsPacket, Message msg, String fileName) {
    // try to find the request

    // check for ixfr/axfr response, the query might be missing from the response
    // so we cannot use the qname for matching.
    RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getDst(),
        dnsPacket.getDstPort());
    if (activeZoneTransfers.containsKey(key)) {
      if (log.isDebugEnabled()) {
        log.debug("Ignore {} zone transfer response(s)", msg.getAnswer().size());
      }
      // this response is part of an active zonetransfer.
      // only let the first response continue, reuse the "time" field of the RequestKey to
      // keep track of this.
      Integer ztResponseCounter = activeZoneTransfers.get(key);
      if (ztResponseCounter.intValue() > 0) {
        // do not save this msg, drop it here, continue with next msg.
        return;
      } else {
        // 1st response msg let it continue, add 1 to the map the indicate 1st resp msg
        // has been processed
        activeZoneTransfers.put(key, 1);
      }
    }

    key = new RequestCacheKey(msg.getHeader().getId(), qname(msg), dnsPacket.getDst(),
        dnsPacket.getDstPort());
    RequestCacheValue request = requestCache.remove(key);
    // check to see if the request msg exists, at the start of the pcap there may be
    // missing queries

    if (request != null && request.getPacket() != null && request.getMessage() != null) {

      pushRow(
          new RowData(request.getPacket(), request.getMessage(), dnsPacket, msg, false, fileName));

    } else {
      // no request found, this could happen if the query was in previous pcap
      // and was not correctly decoded, or the request timed out before server
      // could send a response.
      if (log.isDebugEnabled()) {
        log.debug("Found no request for response");
      }

      pushRow(new RowData(null, null, dnsPacket, msg, false, fileName));
    }
  }

  private void pushRow(RowData pc) {
    try {
      // the put() method will block until enough space is available in the queue
      // to prevent having large amount of rows in memory when the writer cannot keep up.
      rowQueue.put(pc);
    } catch (InterruptedException e) {
      // ignoire error, just log
      log.error("Error while sending packet to writer queue.");
    }
  }

  /**
   * Save the loader state with incomplete datagrams, tcp streams and unmatched dns queries to disk.
   */
  private void persistState() {
    log.info("Write internal state to file");

    int datagramCount = 0;
    int cacheCount = 0;

    try {
      // persist tcp state
      stateManager.write(flows);

      if (datagrams != null) {
        // persist IP datagrams
        Map<Datagram, Collection<DatagramPayload>> outMap = new HashMap<>();
        for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : datagrams
            .asMap()
            .entrySet()) {
          Collection<DatagramPayload> datagrams2persist = new ArrayList<>();
          for (DatagramPayload sequencePayload : entry.getValue()) {
            datagrams2persist.add(sequencePayload);
          }
          outMap.put(entry.getKey(), datagrams2persist);
          datagramCount++;
        }

        stateManager.write(outMap);
      }

      if (requestCache != null) {
        // persist request cache
        stateManager.write(requestCache);
        cacheCount = requestCache.size();
      }

      // flush metrics to make sure that metrics that can be sent already are sent
      // always send historical stats to monitoring
      if (metrics) {
        metricManager.persistState(stateManager);
      }

    } catch (Exception e) {
      log.error("Error writing state file", e);
      // delete old corrupt state
      stateManager.delete();
    } finally {
      stateManager.close();
    }



    log.info("------------- State persistence stats --------------");
    log.info("Persist {} TCP flows", flows.size());
    log.info("Persist {} Datagrams", datagramCount);
    log.info("Persist {} DNS requests from cache", cacheCount);
    log.info("Persist {} unsent metrics", metricManager.size());
    log.info("----------------------------------------------------");
  }

  @SuppressWarnings("unchecked")
  private void loadState() {
    log.info("Load internal state from file");

    try {
      if (!stateManager.stateAvailable()) {
        log.info("No state file found, do not try to load previous state");
        return;
      }

      // read persisted TCP sessions
      flows = (Map<TCPFlow, FlowData>) stateManager.read();

      // read persisted IP datagrams
      HashMap<Datagram, Collection<DatagramPayload>> inMap =
          (HashMap<Datagram, Collection<DatagramPayload>>) stateManager.read();
      for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : inMap.entrySet()) {
        for (DatagramPayload dgPayload : entry.getValue()) {
          datagrams.put(entry.getKey(), dgPayload);
        }
      }

      // read in previous request cache
      requestCache = (Map<RequestCacheKey, RequestCacheValue>) stateManager.read();

      if (metrics) {
        metricManager.loadState(stateManager);
      }
    } catch (Exception e) {
      log.error("Error reading state file", e);
      // delete old corrupt state
      stateManager.delete();
    } finally {
      stateManager.close();
    }

    log.info("------------- Loader state stats ------------------");
    log.info("Loaded TCP state {} TCP flows", flows.size());
    log.info("Loaded Datagram state {} Datagrams", datagrams.size());
    log.info("Loaded Request cache {} DNS requests", requestCache.size());
    log.info("Loaded metrics state {} metrics", metricManager.size());
    log.info("----------------------------------------------------");
  }


  private void purgeCache() {
    // remove expired entries from _requestCache
    Iterator<RequestCacheKey> iter = requestCache.keySet().iterator();
    long now = System.currentTimeMillis();
    int purgeCounter = 0;

    while (iter.hasNext()) {
      RequestCacheKey key = iter.next();
      // add the expiration time to the key and see if this leads to a time which is after the
      // current time.
      if ((key.getTime() + cacheTimeout) <= now) {
        // remove expired request
        RequestCacheValue cacheValue = requestCache.get(key);
        iter.remove();

        if (cacheValue.getMessage() != null
            && cacheValue.getMessage().getHeader().getQr() == MessageType.QUERY) {

          pushRow(new RowData(cacheValue.getPacket(), cacheValue.getMessage(), null, null, true,
              cacheValue.getFilename()));

          if (log.isDebugEnabled()) {
            log
                .debug("Expired query for: "
                    + cacheValue.getMessage().getQuestions().get(0).getQName());
          }
          purgeCounter++;
        }
      }
    }

    log
        .info("Marked " + purgeCounter
            + " expired queries from request cache to output file with rcode no response");
  }

  private boolean createReader(String file) {
    FileManager fm = fileManagerFactory.getFor(file);
    Optional<InputStream> ois = fm.open(file);

    if (!ois.isPresent()) {
      // cannot create reader, continue with next file
      log.error("Error opening pcap file: " + file);
      return false;
    }

    try {
      InputStream decompressor =
          CompressionUtil.getDecompressorStreamWrapper(ois.get(), file, bufferSizeConfig * 1024);
      this.pcapReader = new PcapReader(
          new DataInputStream(new BufferedInputStream(decompressor, bufferSizeConfig * 1024)));
    } catch (IOException e) {
      log.error("Error creating pcap reader for: " + file, e);
      return false;
    }

    // set the state of the reader, this can be loaded from disk and given
    // to the 1st reader. or it can be result of reader x and it is given to reader y
    // to have state continuity across readers.
    pcapReader.setFlows(flows);
    pcapReader.setDatagrams(datagrams);

    return true;
  }

  private List<String> scan() {
    // if server name is provided then search that location for input files.
    // otherwise search root of inputDir
    String inputDir = serverCtx.getServerInfo().isDefaultServer() ? inputLocation
        : FileUtil.appendPath(inputLocation, serverCtx.getServerInfo().getName());

    FileManager fm = fileManagerFactory.getFor(inputDir);

    log.info("Scan for pcap files in: {}", inputDir);

    inputDir = StringUtils
        .appendIfMissing(inputDir, System.getProperty("file.separator"),
            System.getProperty("file.separator"));

    // order and skip the newest file if skipfirst is true
    List<String> files = fm
        .files(inputDir, ".pcap", ".pcap.gz", ".pcap.xz")
        .stream()
        .sorted()
        .skip(skipFirst ? 1 : 0)
        .collect(Collectors.toList());

    log.info("Found {} file to process", files.size());
    if (log.isDebugEnabled()) {
      files.stream().forEach(file -> log.debug("Found file: {}", file));
    }
    return files;
  }

}

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
import java.io.File;
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
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.service.FileArchiveService;
import nl.sidnlabs.entrada.service.PartitionService;
import nl.sidnlabs.entrada.support.RequestCacheKey;
import nl.sidnlabs.entrada.support.RequestCacheValue;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.entrada.util.CompressionUtil;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.SequencePayload;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Datagram;
import nl.sidnlabs.pcap.packet.DatagramPayload;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;
import nl.sidnlabs.pcap.packet.TCPFlow;

@Log4j2
@Component
public class PacketProcessor {

  private static final int MAX_QUEUE_SIZE = 100000;

  @Value("${entrada.cache.timeout}")
  private int cacheTimeoutConfig;

  @Value("${entrada.cache.timeout.tcp.flows}")
  private int cacheTimeoutTCPConfig;

  @Value("${entrada.cache.timeout.ip.fragmented}")
  private int cacheTimeoutIPFragConfig;

  @Value("${entrada.inputstream.buffer:32}")
  private int bufferSizeConfig;

  @Value("${entrada.icmp.enable}")
  private boolean icmpEnabled;

  @Value("${entrada.location.work}")
  private String workLocation;

  @Value("${entrada.location.input}")
  private String inputLocation;

  @Value("${entrada.location.output}")
  private String outputLocation;

  @Value("${entrada.database.table.dns}")
  private String tableNameDns;

  @Value("${entrada.database.table.icmp}")
  private String tableNameIcmp;

  @Value("${entrada.input.file.skipfirst}")
  private boolean skipFirst;

  private PcapReader pcapReader;
  protected Map<RequestCacheKey, RequestCacheValue> requestCache;

  private PersistenceManager persistenceManager;
  private OutputWriter outputWriter;
  private FileArchiveService fileArchiveService;

  // metrics
  private Counter packetCounter;
  private Counter dnsQueryCounter;
  private Counter dnsResponseCounter;
  private Counter fileCounter;
  private Counter purgeCounter;
  // counter when no request query can be found for a response
  private Counter noQueryFoundCounter;

  // max lifetime for cached packets, in milliseconds (configured in minutes)
  private int cacheTimeout;

  // keep list of active zone transfers
  private Map<RequestCacheKey, Integer> activeZoneTransfers;

  private FileManagerFactory fileManagerFactory;
  private QueryEngine queryEngine;
  private PartitionService partitionService;
  private ServerContext serverCtx;

  private Future<Map<String, Set<Partition>>> outputFuture;
  private LinkedBlockingQueue<RowData> packetQueue;

  private MeterRegistry registry;

  // aps with state, loaded at start and persisted at end
  private Multimap<TCPFlow, SequencePayload> flows = TreeMultimap.create();
  private Multimap<Datagram, DatagramPayload> datagrams = TreeMultimap.create();

  public PacketProcessor(ServerContext serverCtx, PersistenceManager persistenceManager,
      OutputWriter outputWriter, FileArchiveService fileArchiveService,
      FileManagerFactory fileManagerFactory, QueryEngine queryEngine,
      PartitionService partitionService, MeterRegistry registry) {

    this.serverCtx = serverCtx;
    this.persistenceManager = persistenceManager;
    this.outputWriter = outputWriter;
    this.fileArchiveService = fileArchiveService;
    this.fileManagerFactory = fileManagerFactory;
    this.queryEngine = queryEngine;
    this.partitionService = partitionService;

    // convert minutes to seconds
    this.cacheTimeout = 1000 * 60 * cacheTimeoutConfig;
    this.registry = registry;
  }


  /**
   * 
   * Remove old work dir.
   * 
   * @return True if work location for name server is deleted or if it did not exist yet
   */
  private boolean clean() {
    String path = FileUtil.appendPath(workLocation, serverCtx.getServerInfo().getNormalizeName());
    FileManager fm = fileManagerFactory.getFor(path);
    if (fm.exists(path)) {
      return fm.rmdir(path);
    }

    return true;
  }


  public void execute() {
    if (!clean()) {
      // cannot remove old data
      log.error("Cleanup error, stop.");
      return;
    }
    // reset all counters and reused data structures
    reset();

    // search for input files
    List<String> inputFiles = scan();
    if (inputFiles.isEmpty()) {
      // no files found to process, stop
      log.info("No files found, stop.");
      return;
    }

    // get the state from the previous run
    loadState();

    for (String file : inputFiles) {
      Date start = new Date();

      if (fileArchiveService.exists(file, serverCtx.getServerInfo().getFullname())) {
        log.info("file {} already processed!, continue with next file", file);
        // move the pcap file to archive location or delete
        fileArchiveService.archive(file, start, 0);
        continue;
      }

      fileCounter.increment();

      if (outputFuture == null) {
        // open the output file writer
        outputFuture = outputWriter.start(true, icmpEnabled, packetQueue);
      }
      read(file);
      // flush expired packets after every file, to avoid a large cache eating up the heap
      purgeCache();
      // move the pcap file to archive location or delete
      fileArchiveService.archive(file, start, (int) packetCounter.count());
    }

    // check if any file have been processed if so, send "end" packet to writer and wait foor writer
    // to finish
    if (Objects.nonNull(outputFuture)) {
      // save unmatched packet state to file
      // the next pcap might have the missing responses
      persistState();
      // mark all data procssed
      pushPacket(RowData.NULL);
      // wait until writer is done
      log.info("Wait until output writer(s) have finished");
      Map<String, Set<Partition>> partitions = waitForWriter(outputFuture);
      log.info("Output writer(s) have finished, continue uploading results");
      // upload newly created data to fs
      upload(partitions);
      register(partitions);

      logStats();
    }
  }

  private void register(Map<String, Set<Partition>> partitions) {
    partitions.entrySet().stream().forEach(e -> partitionService.create(e.getKey(), e.getValue()));
  }

  private Map<String, Set<Partition>> waitForWriter(
      Future<Map<String, Set<Partition>>> outputFuture) {

    try {
      return outputFuture.get();
    } catch (Exception e) {
      // ignore this error, return empty set
      log.error("Error while waiting for output writer to finish");
    }

    return Collections.emptyMap();
  }

  private void reset() {
    fileCounter =
        registry.counter("processor.pcap", "ns", serverCtx.getServerInfo().getNormalizeName());

    packetCounter =
        registry.counter("processor.packet", "ns", serverCtx.getServerInfo().getNormalizeName());

    dnsQueryCounter =
        registry.counter("packet.dns.query", "ns", serverCtx.getServerInfo().getNormalizeName());

    dnsResponseCounter =
        registry.counter("packet.dns.response", "ns", serverCtx.getServerInfo().getNormalizeName());

    purgeCounter =
        registry.counter("packet.purge", "ns", serverCtx.getServerInfo().getNormalizeName());

    noQueryFoundCounter =
        registry.counter("packet.noquery", "ns", serverCtx.getServerInfo().getNormalizeName());

    requestCache = new HashMap<>();
    activeZoneTransfers = new HashMap<>();
    outputFuture = null;
    packetQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
  }

  private void logStats() {
    log.info("--------- Done processing data-----------");
    log.info("{} packets", (int) (dnsQueryCounter.count() + dnsResponseCounter.count()));
    log.info("{} query packets", (int) dnsQueryCounter.count());
    log.info("{} response packets", (int) dnsResponseCounter.count());
    log.info("{} response packets without request", (int) noQueryFoundCounter.count());
    log.info("-----------------------------------------");
  }

  /**
   * Move created data from the work location to the output location. The original data will be
   * deleted.
   */
  private void upload(Map<String, Set<Partition>> partitions) {
    FileManager fmOutput = fileManagerFactory.getFor(outputLocation);

    // move dns data to the database location on local or remote fs
    String location = locationForDNS();
    FileManager fmInput = fileManagerFactory.getFor(location);
    if (new File(location).exists()) {
      // delete .crc files
      cleanup(fmInput, location, partitions.get("dns"));

      String dstLocation = FileUtil.appendPath(outputLocation, tableNameDns);
      if (fmOutput.upload(location, dstLocation, false)) {
        /*
         * make sure the database table contains all the required partitions. If not create the
         * missing database partition(s)
         */
        queryEngine.addPartition(tableNameDns, partitions.get("dns"));

        log.info("Delete work location: {}", location);
        fmInput.rmdir(location);
      }
    }

    if (icmpEnabled) {
      // move icmp data
      location = locationForICMP();
      if (new File(location).exists()) {
        // delete .crc files
        cleanup(fmInput, location, partitions.get("icmp"));

        String dstLocation = FileUtil.appendPath(outputLocation, tableNameIcmp);
        if (fmOutput.upload(location, dstLocation, false)) {

          queryEngine.addPartition(tableNameIcmp, partitions.get("icmp"));

          log.info("Delete work location: {}", location);
          fmInput.rmdir(location);
        }
      }
    }
  }

  /**
   * Cleanup generated data, parquet-mr generates .crc files, these files should not be uploaded.
   * 
   * @param fm filemanager to use
   * @param location location of the generated data
   * @param partitions created partitions
   */
  private void cleanup(FileManager fm, String location, Set<Partition> partitions) {
    partitions
        .stream()
        .forEach(
            p -> fm.files(FileUtil.appendPath(location, p.toPath()), ".crc").forEach(fm::delete));
  }

  private String locationForDNS() {
    return FileUtil.appendPath(workLocation, serverCtx.getServerInfo().getNormalizeName(), "dns/");
  }

  private String locationForICMP() {
    return FileUtil.appendPath(workLocation, serverCtx.getServerInfo().getNormalizeName(), "icmp/");
  }

  private void read(String file) {
    // try to open file, if file is not good pcap handle exception and fail fast.
    if (!createReader(file)) {
      log.error("Skip bad input file: " + file);
      return;
    }

    long readStart = System.currentTimeMillis();
    log.info("Start reading packet queue");

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
    packetCounter.increment();
    if (packetCounter.count() % 100000 == 0) {
      log.info("Processed " + (int) packetCounter.count() + " packets");
    }

    if (isICMP(currentPacket)) {
      if (!icmpEnabled) {
        // do not process ICMP packets
        return;
      }
      // handle icmp
      pushPacket(new RowData(currentPacket, null, null, null, false, fileName));

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
    dnsQueryCounter.increment();

    // check for ixfr/axfr request
    if (!msg.getQuestions().isEmpty()
        && (msg.getQuestions().get(0).getQType() == ResourceRecordType.AXFR
            || msg.getQuestions().get(0).getQType() == ResourceRecordType.IXFR)) {

      if (log.isDebugEnabled()) {
        log.debug("Detected zonetransfer for: " + dnsPacket.getFlow());
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
    dnsResponseCounter.increment();

    // check for ixfr/axfr response, the query might be missing from the response
    // so we cannot use the qname for matching.
    RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getDst(),
        dnsPacket.getDstPort());
    if (activeZoneTransfers.containsKey(key)) {
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

      pushPacket(
          new RowData(request.getPacket(), request.getMessage(), dnsPacket, msg, false, fileName));

    } else {
      // no request found, this could happen if the query was in previous pcap
      // and was not correctly decoded, or the request timed out before server
      // could send a response.
      log.debug("Found no request for response");
      noQueryFoundCounter.increment();;

      pushPacket(new RowData(null, null, dnsPacket, msg, false, fileName));
    }
  }

  private void pushPacket(RowData pc) {
    try {
      packetQueue.put(pc);
    } catch (InterruptedException e) {
      // ignoire error, just log
      log.error("Error while sending packet to writer queue.");
    }
  }

  /**
   * Save the loader state with incomplete datagrams, tcp streams and unmatched dns queries to disk.
   */
  private void persistState() {
    int flowCount = 0;
    int datagramCount = 0;
    int cacheCount = 0;

    if (flows != null) {
      // persist tcp state
      Map<TCPFlow, Collection<SequencePayload>> pmap = new HashMap<>();
      for (Map.Entry<TCPFlow, Collection<SequencePayload>> entry : flows.asMap().entrySet()) {
        Collection<SequencePayload> payloads = new ArrayList<>();
        for (SequencePayload sequencePayload : entry.getValue()) {
          payloads.add(sequencePayload);
        }
        pmap.put(entry.getKey(), payloads);
        flowCount++;
      }

      persistenceManager.write(pmap);
    }

    if (datagrams != null) {
      // persist IP datagrams
      Map<Datagram, Collection<DatagramPayload>> outMap = new HashMap<>();
      for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : datagrams.asMap().entrySet()) {
        Collection<DatagramPayload> datagrams2persist = new ArrayList<>();
        for (DatagramPayload sequencePayload : entry.getValue()) {
          datagrams2persist.add(sequencePayload);
        }
        outMap.put(entry.getKey(), datagrams2persist);
        datagramCount++;
      }

      persistenceManager.write(outMap);
    }

    if (requestCache != null) {
      // persist request cache
      persistenceManager.write(requestCache);
      cacheCount = requestCache.size();
    }

    persistenceManager.close();

    log.info("------------- State persistence stats --------------");
    log.info("Persist {} TCP flows", flowCount);
    log.info("Persist {} Datagrams", datagramCount);
    log.info("Persist {} DNS requests from cache", cacheCount);
    log.info("----------------------------------------------------");
  }

  @SuppressWarnings("unchecked")
  private void loadState() {

    if (!persistenceManager.stateAvailable()) {
      log.info("No state file found, do not try to load previous state");
      return;
    }

    // read persisted TCP sessions
    Map<TCPFlow, Collection<SequencePayload>> map = persistenceManager.read(HashMap.class);
    for (Map.Entry<TCPFlow, Collection<SequencePayload>> entry : map.entrySet()) {
      for (SequencePayload sequencePayload : entry.getValue()) {
        flows.put(entry.getKey(), sequencePayload);
      }
    }
    // read persisted IP datagrams
    HashMap<Datagram, Collection<DatagramPayload>> inMap = persistenceManager.read(HashMap.class);
    for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : inMap.entrySet()) {
      for (DatagramPayload dgPayload : entry.getValue()) {
        datagrams.put(entry.getKey(), dgPayload);
      }
    }

    // read in previous request cache
    requestCache = persistenceManager.read(HashMap.class);

    persistenceManager.close();


    log.info("------------- Loader state stats ------------------");
    log.info("Loaded TCP state {} TCP flows", flows.size());
    log.info("Loaded Datagram state {} Datagrams", datagrams.size());
    log.info("Loaded Request cache {} DNS requests", requestCache.size());
    log.info("----------------------------------------------------");
  }


  private void purgeCache() {
    // remove expired entries from _requestCache
    Iterator<RequestCacheKey> iter = requestCache.keySet().iterator();
    long now = System.currentTimeMillis();

    while (iter.hasNext()) {
      RequestCacheKey key = iter.next();
      // add the expiration time to the key and see if this leads to a time which is after the
      // current time.
      if ((key.getTime() + cacheTimeout) <= now) {
        // remove expired request
        RequestCacheValue mw = requestCache.get(key);
        iter.remove();

        if (mw.getMessage() != null && mw.getMessage().getHeader().getQr() == MessageType.QUERY) {

          pushPacket(
              new RowData(mw.getPacket(), mw.getMessage(), null, null, true, mw.getFilename()));

          purgeCounter.increment();

        } else {
          log.debug("Cached response entry timed out, request might have been missed");
          noQueryFoundCounter.increment();
        }
      }
    }

    log
        .info("Marked " + purgeCounter
            + " expired queries from request cache to output file with rcode no response");
  }

  private boolean createReader(String file) {
    log.info("Start loading queue from file:" + file);

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

    // set the state of the reader
    pcapReader.setFlows(flows);
    pcapReader.setDatagrams(datagrams);

    return true;
  }

  private List<String> scan() {
    // if server name is provided then search that location for input files.
    // otherwise search inputDir
    String inputDir = serverCtx.getServerInfo().getFullname().length() > 0
        ? FileUtil.appendPath(inputLocation, serverCtx.getServerInfo().getFullname())
        : inputLocation;

    FileManager fm = fileManagerFactory.getFor(inputDir);

    log.info("Scan for pcap files in: {}", inputDir);

    inputDir = StringUtils
        .appendIfMissing(inputDir, System.getProperty("file.separator"),
            System.getProperty("file.separator"));

    List<String> files = fm.files(inputDir, ".pcap", ".pcap.gz", ".pcap.xz");

    if (skipFirst && fm.isLocal()) {
      // order by date and skip the youngest file
      // do this onlu for local fs
      List<File> sorted = files
          .stream()
          .map(File::new)
          .sorted(LastModifiedFileComparator.LASTMODIFIED_REVERSE)
          .skip(1)
          .collect(Collectors.toList());


      files = sorted.stream().map(File::toString).collect(Collectors.toList());
    } else {
      // sort the files by name, tcp streams and udp fragmentation may overlap multiple files.
      // so ordering is important.
      Collections.sort(files);
    }

    log.info("Found {} file to process:", files.size());
    files.stream().forEach(file -> log.info("Found file: {}", file));

    return files;
  }

}

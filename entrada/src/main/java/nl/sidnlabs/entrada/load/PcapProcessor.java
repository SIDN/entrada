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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import lombok.extern.log4j.Log4j2;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.types.MessageType;
import nl.sidn.dnslib.types.ResourceRecordType;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.SequencePayload;
import nl.sidn.pcap.decoder.ICMPDecoder;
import nl.sidn.pcap.packet.DNSPacket;
import nl.sidn.pcap.packet.Datagram;
import nl.sidn.pcap.packet.DatagramPayload;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.packet.TCPFlow;
import nl.sidnlabs.entrada.config.Settings;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.metric.MetricManager;
import nl.sidnlabs.entrada.parquet.ParquetOutputHandler;
import nl.sidnlabs.entrada.support.MessageWrapper;
import nl.sidnlabs.entrada.support.PacketCombination;
import nl.sidnlabs.entrada.support.RequestKey;
import nl.sidnlabs.entrada.util.CompressionUtil;

@Log4j2
@Component
public class PcapProcessor {

  private static final int DEFAULT_PCAP_READER_BUFFER_SIZE = 65536;

  // config options from application.properties
  @Value("${entrada.cache.timeout}")
  private int cacheTimeoutConfig;

  @Value("${entrada.cache.timeout.tcp.flows}")
  private int cacheTimeoutTCPConfig;

  @Value("${entrada.cache.timeout.ip.fragmented}")
  private int cacheTimeoutIPFragConfig;

  @Value("${entrada.buffer.pcap.reader}")
  private int bufferSizeConfig;

  private PcapReader pcapReader;
  protected Map<RequestKey, MessageWrapper> requestCache = new HashMap<>();

  private List<String> processedFiles = new ArrayList<>();
  private List<String> inputFiles = new ArrayList<>();

  private Settings settings;
  private MetricManager metricManager;
  private PersistenceManager persistenceManager;
  private ParquetOutputHandler outputHandler;

  private int multiCounter;

  // metrics
  private int queryCounter;
  private int responseCounter;
  private int fileCount = 0;
  private int purgeCounter = 0;
  // counter when no request query can be found for a response
  private int noQueryFoundCounter = 0;

  // max lifetime for cached packets, in milliseconds (configured in minutes)
  private int cacheTimeout;

  // keep list of active zone transfers
  private Map<RequestKey, Integer> activeZoneTransfers = new HashMap<>();

  public PcapProcessor(Settings settings, MetricManager metricManager,
      PersistenceManager persistenceManager, ParquetOutputHandler outputHandler) {
    this.settings = settings;
    this.metricManager = metricManager;
    this.persistenceManager = persistenceManager;
    this.outputHandler = outputHandler;

    // convert minutes to seconds
    this.cacheTimeout = 1000 * 60 * cacheTimeoutConfig;
    this.pcapReader = new PcapReader();
  }


  public void execute() {
    // search for input files
    scan();
    if (inputFiles.isEmpty()) {
      // no files found to process, stop
      log.info("No files found, stop.");
      return;
    }
    // get the state from the previous run
    loadState();
    // open the output file writer
    outputHandler.open();
    for (String file : inputFiles) {
      read(file);
      // flush expired packets after every file, to avoid a cache explosion
      purgeCache();
      archiveFile(file);
    }
    // save unmatched packet state to file
    // the next pcap might have the missing responses
    persistState();

    log.info("--------- Done loading queue ------------");
    log.info("Loaded {} packets", queryCounter + responseCounter);
    log.info("Loaded {} query packets", queryCounter);
    log.info("Loaded {} response packets", responseCounter);
    log.info("Loaded {} messages from TCP streams with > 1 mesg", multiCounter);
    log.info("Found {} response packets without request", noQueryFoundCounter);
    log.info("-----------------------------------------");

    writeMetrics();
    // close file handler(s)
    outputHandler.close();
  }

  /**
   * Write the loader metrics to the metrics queue
   */
  private void writeMetrics() {

    metricManager.send(MetricManager.METRIC_IMPORT_FILES_COUNT, fileCount);
    metricManager.send(MetricManager.METRIC_IMPORT_DNS_NO_REQUEST_COUNT, noQueryFoundCounter);
    metricManager.send(MetricManager.METRIC_IMPORT_DNS_TCPSTREAM_COUNT, multiCounter);
    metricManager
        .send(MetricManager.METRIC_IMPORT_STATE_PERSIST_UDP_FLOW_COUNT,
            pcapReader.getDatagrams().size());
    metricManager
        .send(MetricManager.METRIC_IMPORT_STATE_PERSIST_TCP_FLOW_COUNT,
            pcapReader.getFlows().size());
    metricManager.send(MetricManager.METRIC_IMPORT_STATE_PERSIST_DNS_COUNT, requestCache.size());
    metricManager
        .send(MetricManager.METRIC_IMPORT_TCP_PREFIX_ERROR_COUNT, pcapReader.getTcpPrefixError());
    metricManager
        .send(MetricManager.METRIC_IMPORT_DNS_DECODE_ERROR_COUNT, pcapReader.getDnsDecodeError());
  }

  private void archiveFile(String pcap) {
    File file = new File(pcap);
    File archiveDir =
        new File(settings.getOutputDir() + System.getProperty("file.separator") + "archive"
            + System.getProperty("file.separator") + settings.getServerInfo().getFullname());
    if (!archiveDir.exists()) {
      log.info(archiveDir.getName() + " does not exist, create now.");
      if (!archiveDir.mkdirs()) {
        throw new ApplicationException("creating archive dir: " + archiveDir.getAbsolutePath());
      }
    }
    File newFile =
        new File(archiveDir.getPath() + System.getProperty("file.separator") + file.getName());
    try {
      Files.move(Paths.get(file.getAbsolutePath()), Paths.get(newFile.getAbsolutePath()));
      log.info(pcap + " is archived!");
    } catch (Exception e) {
      throw new ApplicationException("Error moving " + pcap + " to the archive: " + e);
    }
  }

  private String extractPcapFile(String file) {
    try {
      return FileUtils.getFile(file).getName();
    } catch (Exception e) {
      // some problem, cannot get file, use "unknown" to signal this
      return "unknown??   n                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             mm";
    }
  }

  public void read(String file) {
    // try to open file, if file is not good pcap handle exception and fail fast.
    if (!createReader(file)) {
      log.error("Skip bad input file: " + file);
      return;
    }

    long readStart = System.currentTimeMillis();
    log.info("Start reading packet queue");

    // get filename only to map parquet row to pcap file
    String fileName = extractPcapFile(file);

    long counter = 0;
    for (Packet currentPacket : pcapReader) {
      counter++;
      if (counter % 100000 == 0) {
        log.info("Processed " + counter + " packets");
      }
      if (currentPacket != null && currentPacket.getIpVersion() != 0) {

        if ((currentPacket.getProtocol() == ICMPDecoder.PROTOCOL_ICMP_V4)
            || (currentPacket.getProtocol() == ICMPDecoder.PROTOCOL_ICMP_V6)) {
          // handle icmp
          outputHandler
              .handle(new PacketCombination(currentPacket, null, settings.getServerInfo(), null,
                  null, false, fileName));

        } else {
          DNSPacket dnsPacket = (DNSPacket) currentPacket;
          if (dnsPacket.getMessage() == null) {
            // skip malformed packets
            log.debug("Drop packet with no dns message");
            continue;
          }

          if (dnsPacket.getMessageCount() > 1) {
            multiCounter = multiCounter + dnsPacket.getMessageCount();
          }

          for (Message msg : dnsPacket.getMessages()) {
            // get qname from request which is part of the cache lookup key
            String qname = null;
            if (msg != null && msg.getQuestions() != null && !msg.getQuestions().isEmpty()) {
              qname = msg.getQuestions().get(0).getQName();
            }

            // put request into map until we find matching response, with a key based on: query id,
            // qname, ip src, tcp/udp port
            // add time for possible timeout eviction
            if (msg.getHeader().getQr() == MessageType.QUERY) {
              queryCounter++;

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
                    .put(new RequestKey(msg.getHeader().getId(), null, dnsPacket.getSrc(),
                        dnsPacket.getSrcPort()), 0);
              }

              RequestKey key = new RequestKey(msg.getHeader().getId(), qname, dnsPacket.getSrc(),
                  dnsPacket.getSrcPort(), System.currentTimeMillis());
              requestCache.put(key, new MessageWrapper(msg, dnsPacket, fileName));
            } else {
              // try to find the request
              responseCounter++;

              // check for ixfr/axfr response, the query might be missing from the response
              // so we cannot use the qname for matching.
              RequestKey key = new RequestKey(msg.getHeader().getId(), null, dnsPacket.getDst(),
                  dnsPacket.getDstPort());
              if (activeZoneTransfers.containsKey(key)) {
                // this response is part of an active zonetransfer.
                // only let the first response continue, reuse the "time" field of the RequestKey to
                // keep track of this.
                Integer ztResponseCounter = activeZoneTransfers.get(key);
                if (ztResponseCounter.intValue() > 0) {
                  // do not save this msg, drop it here, continue with next msg.
                  continue;
                } else {
                  // 1st response msg let it continue, add 1 to the map the indicate 1st resp msg
                  // has been processed
                  activeZoneTransfers.put(key, 1);
                }
              }

              key = new RequestKey(msg.getHeader().getId(), qname, dnsPacket.getDst(),
                  dnsPacket.getDstPort());
              MessageWrapper request = requestCache.remove(key);
              // check to see if the request msg exists, at the start of the pcap there may be
              // missing queries

              if (request != null && request.getPacket() != null && request.getMessage() != null) {

                outputHandler
                    .handle(new PacketCombination(request.getPacket(), request.getMessage(),
                        settings.getServerInfo(), dnsPacket, msg, false, fileName));

              } else {
                // no request found, this could happen if the query was in previous pcap
                // and was not correctly decoded, or the request timed out before server
                // could send a response.
                log.debug("Found no request for response");
                noQueryFoundCounter++;

                outputHandler
                    .handle(new PacketCombination(null, null, settings.getServerInfo(), dnsPacket,
                        msg, false, fileName));
              }
            }
          }
        } // end of dns packet
      }
    }
    log.info("Processing time: " + (System.currentTimeMillis() - readStart) + "ms");
    if (log.isDebugEnabled()) {
      log.debug("Done with decoding, start cleanup");
    }
    // clear expired cache entries
    pcapReader.clearCache(cacheTimeoutTCPConfig * 60 * 1000, cacheTimeoutIPFragConfig * 60 * 1000);
    pcapReader.close();
  }

  /**
   * Save the loader state with incomplete datagrams, tcp streams and unmatched dns queries to disk.
   */
  private void persistState() {

    Map<TCPFlow, Collection<SequencePayload>> pmap = new HashMap<>();
    // persist tcp state
    Map<TCPFlow, Collection<SequencePayload>> flows = pcapReader.getFlows().asMap();
    // convert to std java map and collection
    Iterator<TCPFlow> iter = flows.keySet().iterator();
    while (iter.hasNext()) {
      TCPFlow tcpFlow = iter.next();
      Collection<SequencePayload> payloads = new ArrayList<>();
      Collection<SequencePayload> payloads2Persist = flows.get(tcpFlow);
      for (SequencePayload sequencePayload : payloads2Persist) {
        payloads.add(sequencePayload);
      }
      pmap.put(tcpFlow, payloads);
    }

    persistenceManager.write(pmap);

    // persist IP datagrams
    Map<Datagram, Collection<DatagramPayload>> datagrams = pcapReader.getDatagrams().asMap();
    // convert to std java map and collection
    Map<Datagram, Collection<DatagramPayload>> outMap = new HashMap<>();

    Iterator<Datagram> ipIter = datagrams.keySet().iterator();
    while (iter.hasNext()) {
      Datagram dg = ipIter.next();
      Collection<DatagramPayload> datagrams2persist = new ArrayList<>();
      Collection<DatagramPayload> datagramPayloads = datagrams.get(dg);
      for (DatagramPayload sequencePayload : datagramPayloads) {
        datagrams2persist.add(sequencePayload);
      }
      outMap.put(dg, datagrams2persist);
    }

    persistenceManager.write(outMap);

    // persist request cache
    persistenceManager.write(requestCache);

    persistenceManager.close();

    log.info("------------- State persistence stats --------------");
    log.info("Persist {} TCP flows", pmap.size());
    log.info("Persist {} Datagrams", pcapReader.getDatagrams().size());
    log.info("Persist request cache {} DNS requests", requestCache.size());
    log.info("----------------------------------------------------");
  }

  @SuppressWarnings("unchecked")
  private void loadState() {

    if (!persistenceManager.stateAvailable()) {
      log.info("No state file found, do not try to load previous state");
      return;
    }

    // read persisted TCP sessions
    Multimap<TCPFlow, SequencePayload> flows = TreeMultimap.create();
    Map<TCPFlow, Collection<SequencePayload>> map = persistenceManager.read(HashMap.class);
    for (Map.Entry<TCPFlow, Collection<SequencePayload>> entry : map.entrySet()) {
      for (SequencePayload sequencePayload : entry.getValue()) {
        flows.put(entry.getKey(), sequencePayload);
      }
    }
    pcapReader.setFlows(flows);

    // read persisted IP datagrams
    Multimap<nl.sidn.pcap.packet.Datagram, nl.sidn.pcap.packet.DatagramPayload> datagrams =
        TreeMultimap.create();
    HashMap<Datagram, Collection<DatagramPayload>> inMap = persistenceManager.read(HashMap.class);
    for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : inMap.entrySet()) {
      for (DatagramPayload dgPayload : entry.getValue()) {
        datagrams.put(entry.getKey(), dgPayload);
      }
    }
    pcapReader.setDatagrams(datagrams);

    // read in previous request cache
    requestCache = persistenceManager.read(HashMap.class);

    persistenceManager.close();


    log.info("------------- Loader state stats ------------------");
    log.info("Loaded TCP state {} TCP flows", pcapReader.getFlows().size());
    log.info("Loaded Datagram state {} Datagrams", pcapReader.getDatagrams().size());
    log.info("Loaded Request cache {} DNS requests", requestCache.size());
    log.info("----------------------------------------------------");
  }


  private void purgeCache() {
    // remove expired entries from _requestCache
    Iterator<RequestKey> iter = requestCache.keySet().iterator();
    long now = System.currentTimeMillis();

    while (iter.hasNext()) {
      RequestKey key = iter.next();
      // add the expiration time to the key and see if this leads to a time which is after the
      // current time.
      if ((key.getTime() + cacheTimeout) <= now) {
        // remove expired request
        MessageWrapper mw = requestCache.get(key);
        iter.remove();

        if (mw.getMessage() != null && mw.getMessage().getHeader().getQr() == MessageType.QUERY) {

          outputHandler
              .handle(new PacketCombination(mw.getPacket(), mw.getMessage(),
                  settings.getServerInfo(), null, null, true, mw.getFilename()));

          purgeCounter++;

        } else {
          log.debug("Cached response entry timed out, request might have been missed");
          noQueryFoundCounter++;
        }
      }
    }

    log
        .info("Marked " + purgeCounter
            + " expired queries from request cache to output file with rcode no response");
  }

  public boolean createReader(String file) {
    log.info("Start loading queue from file:" + file);
    fileCount++;
    try {
      File f = FileUtils.getFile(file);
      log.info("Load data for server: " + settings.getServerInfo().getFullname());

      FileInputStream fis = FileUtils.openInputStream(f);
      int bufSize = bufferSizeConfig > 512 ? bufferSizeConfig : DEFAULT_PCAP_READER_BUFFER_SIZE;

      InputStream decompressor = CompressionUtil.getDecompressorStreamWrapper(fis, file, bufSize);
      pcapReader.init(new DataInputStream(decompressor));
    } catch (IOException e) {
      // cannot create reader, continue with next file
      log.error("Error opening pcap file: " + file, e);
      return false;
    }

    return true;
  }

  private void scan() {
    String inputDir = settings.getInputDir() + System.getProperty("file.separator")
        + settings.getServerInfo().getFullname();

    log.info("Scan for pcap files in: {}", inputDir);

    File f = new File(inputDir);
    if (!f.exists()) {
      throw new ApplicationException("input directory " + inputDir + "  does not exist");
    }

    Iterator<File> files =
        FileUtils.iterateFiles(f, new String[] {"pcap", "pcap.gz", "pcap.xz"}, false);

    while (files.hasNext()) {
      File pcap = files.next();
      String fqn = pcap.getAbsolutePath();
      if (!processedFiles.contains(fqn) && !inputFiles.contains(fqn)) {
        inputFiles.add(fqn);
      }
    }
    // sort the files by name, tcp streams and udp fragmentation may overlap multiple files.
    // so ordering is important.
    Collections.sort(inputFiles);
    for (String file : inputFiles) {
      log.info("Found file: " + file);
    }
  }

}

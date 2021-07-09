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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.typesafe.config.Config;
import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ClosedShape;
import akka.stream.FanOutShape2;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Outlet;
import akka.stream.SinkShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Unzip;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.SharedContext;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.load.stream.RowBuilderOperator;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.ProtocolType;
import nl.sidnlabs.entrada.model.Row;
import nl.sidnlabs.entrada.service.ArchiveService;
import nl.sidnlabs.entrada.service.PartitionService;
import nl.sidnlabs.entrada.service.UploadService;
import nl.sidnlabs.entrada.support.RequestCacheKey;
import nl.sidnlabs.entrada.support.RequestCacheValue;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.entrada.util.CompressionUtil;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.decoder.DNSDecoder;
import nl.sidnlabs.pcap.decoder.ICMPDecoder;
import nl.sidnlabs.pcap.decoder.IPDecoder;
import nl.sidnlabs.pcap.decoder.TCPDecoder;
import nl.sidnlabs.pcap.decoder.UDPDecoder;
import nl.sidnlabs.pcap.packet.Datagram;
import nl.sidnlabs.pcap.packet.DatagramPayload;
import nl.sidnlabs.pcap.packet.FlowData;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.TCPFlow;

@Log4j2
@Component
// use prototype scope, create new bean each time batch of files is processed
// this to avoid problems with memory/caches when running app for a long period of time
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PacketProcessor {

  @Value("${entrada.tcp.enable:true}")
  private boolean tcpEnabled;

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
  private boolean metricsEnabled;

  @Value("#{${entrada.partition.activity.ping:5}*60*1000}")
  private int maxPartitionPingMs;

  @Value("${entrada.parquet.upload.batch}")
  private boolean uploadBatch;

  @Value("${entrada.writer.dns.count:1}")
  private int dnsWriterCount;

  @Value("${entrada.writer.icmp.count:1}")
  private int icmpWriterCount;

  @Value("${entrada.row.builder.count:1}")
  private int rowbuilderCount;

  private PcapReader pcapReader;
  private ApplicationContext applicationContext;
  private StateManager stateManager;
  private ArchiveService fileArchiveService;
  private SharedContext sharedContext;

  private String currentFileName;

  private FileManagerFactory fileManagerFactory;
  private PartitionService partitionService;
  private ServerContext serverCtx;
  private HistoricalMetricManager metricManager;
  private UploadService uploadService;
  // maps with state, loaded at start and persisted at end
  private Map<TCPFlow, FlowData> flows = new HashMap<>();
  private Multimap<Datagram, DatagramPayload> datagrams = TreeMultimap.create();

  private Config akkaConfig;
  private ActorSystem system;
  private ApplicationContext ctx;

  private RowBuilderOperator rowBuilder;
  private Graph<ClosedShape, List<CompletionStage<Done>>> graph;
  private List<RowWriter> writers = new ArrayList<>();
  private PacketJoiner joiner;
  private IPDecoder ipDecoder;

  public PacketProcessor(ServerContext serverCtx, StateManager persistenceManager,
      ArchiveService fileArchiveService, FileManagerFactory fileManagerFactory,
      PartitionService partitionService, HistoricalMetricManager historicalMetricManager,
      UploadService uploadService, SharedContext sharedContext, Config akkaConfig,
      ApplicationContext ctx, ApplicationContext applicationContext, RowBuilderOperator rowBuilder,
      PacketJoiner joiner) {

    this.serverCtx = serverCtx;
    this.stateManager = persistenceManager;
    this.fileArchiveService = fileArchiveService;
    this.fileManagerFactory = fileManagerFactory;
    this.partitionService = partitionService;
    this.uploadService = uploadService;
    this.metricManager = historicalMetricManager;
    this.sharedContext = sharedContext;
    this.applicationContext = applicationContext;
    this.akkaConfig = akkaConfig;
    this.ctx = ctx;
    this.rowBuilder = rowBuilder;
    this.joiner = joiner;
  }

  @PostConstruct
  private void init() {
    // create deocder here, so we can offload the decoding to a separate thread
    TCPDecoder tcpDecoder = null;
    // can share decoders, decoding is single threaded in akka streams
    DNSDecoder dnsDecoder = new DNSDecoder(false);
    if (tcpEnabled) {
      tcpDecoder = new TCPDecoder(dnsDecoder);
    }
    ipDecoder = new IPDecoder(tcpDecoder, new UDPDecoder(dnsDecoder), new ICMPDecoder());
  }

  private void startAkka() {
    system = ActorSystem.create("entrada-akka", akkaConfig);
  }

  private void stopAkka() {
    if (system != null) {
      system.terminate();
    }
  }

  private boolean isAkkaStarted() {
    return system != null;
  }

  private void reset() {
    ipDecoder.reset();
    rowBuilder.reset();
    joiner.reset();
  }

  public void execute() {
    // search for input files
    List<String> inputFiles = scan();
    if (inputFiles.isEmpty()) {
      // no files found to process, stop
      return;
    }

    long partitionCheckTs = System.currentTimeMillis();
    long startAll = System.currentTimeMillis();
    // load the state from the previous run
    loadState();
    int fileCounter = 0;

    Map<String, Set<Partition>> createdPartitions = null;

    for (String file : inputFiles) {
      currentFileName = FileUtil.filename(file);
      long fileSize = FileUtil.size(file);
      Date startDate = new Date();

      if (fileArchiveService.exists(file, serverCtx.getServerInfo().getName())) {
        if (log.isDebugEnabled()) {
          log.debug("file {} already processed!, continue with next file", file);
        }
        // move the pcap file to archive location or delete
        fileArchiveService.archive(file, startDate, 0, fileSize);
        continue;
      }

      if (!isAkkaStarted()) {
        // start akka system
        startAkka();
      }

      // clear counters
      reset();

      fileCounter++;

      long startTs = System.currentTimeMillis();
      read(file);
      long fileProcTime = (System.currentTimeMillis() - startTs);

      double packetsMs = 0;
      double bytessMs = 0;

      if (fileProcTime > 0) {
        packetsMs = joiner.getCounter() / fileProcTime;
        bytessMs = fileSize / fileProcTime;
      }

      printStats(currentFileName, fileProcTime, fileSize, joiner.getCounter(), packetsMs, bytessMs);

      // reset counters
      reset();

      // move the pcap file to archive location or delete
      fileArchiveService.archive(file, startDate, joiner.getCounter(), fileSize);

      // check if we need to upload using batch (when all input files have been processed) or
      // upload all files that are not active anymore before all inout data has been processed
      createdPartitions = createdPartitions();

      if (!uploadBatch) {
        // upload parquet files that are not active anymore (no longer written to)
        upload(createdPartitions);
      }

      // make sure the active partitions are not compacted during bulk loading
      if (pingPartitions(partitionCheckTs, createdPartitions)) {
        // reset timer
        partitionCheckTs = System.currentTimeMillis();
      }

      // check if file porcessing has been cancelled.
      if (!sharedContext.isEnabled()) {
        // processing not enabled break current file processing loop
        log.info("Processing PCAP data is currently not enabled, stopping now");
        break;
      }
      // send metrics for processed file to grafana
      metricManager.flush();
    }

    if (fileCounter > 0) {
      // atleast 1 file has been processed
      stop();

      log
          .info("Processed " + inputFiles.size() + " files, time: "
              + (System.currentTimeMillis() - startAll) + "ms");

      // upload newly created data to fs
      uploadService.upload(createdPartitions, true);
      createPartitions(createdPartitions);

      // save unmatched packet state to file
      // the next pcap might have the missing responses
      persistState();

      stopAkka();
    }

    log.info("Ready, processed {} new files", fileCounter);
  }

  private void printStats(String fileName, long fileProcTime, long fileSize, int packets,
      double packetsMs, double bytessMs) {
    log.info("------------- Processor File Stats -----------------------");
    log.info("File: {}", fileName);
    log.info("Time (ms): {}", fileProcTime);
    log.info("Size (bytes): {}", fileSize);
    log.info("Packets: {}", packets);
    log.info("Packets/ms: {}", packetsMs);
    log.info("Bytes/ms: {}", bytessMs);

    ipDecoder.printStats();
    rowBuilder.printStats();
    for (RowWriter w : writers) {
      w.printStats();
    }
  }

  private void stop() {
    for (RowWriter w : writers) {
      w.close();
    }
  }

  private Map<String, Set<Partition>> createdPartitions() {
    Map<String, Set<Partition>> p = new HashMap<>();
    p.put("dns", new HashSet<>());
    p.put("icmp", new HashSet<>());

    for (RowWriter w : writers) {
      if (w.type() == ProtocolType.DNS) {
        p.get("dns").addAll(w.getPartitions());
      } else {
        p.get("icmp").addAll(w.getPartitions());
      }
    }

    return p;
  }

  private void upload(Map<String, Set<Partition>> partitions) {
    try {
      uploadService.upload(partitions, false);
    } catch (Exception e) {
      log.error("Error while uploading partitions", e);
    }
  }

  /**
   * Update the last updated date for active partitions, to prevent the partition from being
   * compacted when processing a bulk load of pcap data which might take hours and in this time the
   * partition is otherwise not updated in the database.
   * 
   * @param procStart
   * @return
   */
  private boolean pingPartitions(long start, Map<String, Set<Partition>> partitions) {
    long now = System.currentTimeMillis();

    if ((now - start) > maxPartitionPingMs) {
      // last partition ping was > configured for partitionActivePing

      partitions.entrySet().stream().forEach(e -> partitionService.ping(e.getKey(), e.getValue()));
      return true;
    }

    return false;
  }

  private void createPartitions(Map<String, Set<Partition>> partitions) {
    partitions.entrySet().stream().forEach(e -> partitionService.create(e.getKey(), e.getValue()));
  }

  private Graph<ClosedShape, List<CompletionStage<Done>>> createGraph() {

    final String svr = serverCtx.getServerInfo().getNormalizedName();
    final List<Sink<Row, CompletionStage<Done>>> list = new ArrayList<>();

    // create sinks for dns
    for (int i = 0; i < dnsWriterCount; i++) {

      RowWriter w = applicationContext.getBean("parquet-dns", RowWriter.class);
      writers.add(w);

      Sink<Row, CompletionStage<Done>> s = Flow
          .of(Row.class)
          // .buffer(bufferSize, OverflowStrategy.backpressure())
          .toMat(Sink.<Row>foreach(r -> w.write(r, svr)), Keep.right())
          .async("writer-dispatcher");

      list.add(s);
    }

    // create sinks for icmp
    for (int i = 0; i < icmpWriterCount; i++) {

      RowWriter w = applicationContext.getBean("parquet-icmp", RowWriter.class);
      writers.add(w);

      Sink<Row, CompletionStage<Done>> s = Flow
          .of(Row.class)
          // .buffer(bufferSize, OverflowStrategy.backpressure())
          .toMat(Sink.<Row>foreach(r -> w.write(r, svr)), Keep.right())
          .async("writer-dispatcher");

      list.add(s);
    }


    // create reuseable akka streams graph
    return GraphDSL.create(list, (builder, outs) -> {

      // create input source, reading pcap file on dedicated thread
      final Outlet<Packet> IN = builder
          .add(Source
              .fromJavaStream(() -> pcapReader.stream())
              // send last packet when pcap stream is done to let downstream operators cleanup/close
              .concat(Source.single(Packet.LAST))
              .async("reader-dispatcher")
              .log("Akka-SRC"))
          .out();

      // add step to decode packets (this is not threadsafe, don't exec parallel
      // use single pinneddispatcher to prevent thread context-switch overhead
      final FlowShape<Packet, Packet> DECODE = builder
          .add(Flow
              .of(Packet.class)
              // .buffer(bufferSize, OverflowStrategy.backpressure())
              .map(p -> ipDecoder.decode(p))
              .async("decoder-dispatcher"));

      // add step to join packets (this is not threadsafe, don't exec parallel
      final FlowShape<Packet, RowData> JOIN = builder
          .add(Flow
              .of(Packet.class)
              // .buffer(bufferSize, OverflowStrategy.backpressure())
              .mapConcat(p -> joiner.join(p)));

      // run rowbuilder on seperate theads from row-builder-dispatcher
      final Executor rowBuilderEx = system.dispatchers().lookup("row-builder-dispatcher");
      // building rows can be done parallel to increase throughput
      final FlowShape<RowData, Pair<Row, List>> BUILD_ROW = builder
          .add(Flow
              .of(RowData.class)
              // .buffer(bufferSize, OverflowStrategy.backpressure())
              .mapAsyncUnordered(rowbuilderCount, rd -> CompletableFuture
                  .supplyAsync(() -> rowBuilder.process(rd, svr), rowBuilderEx)));

      // send rows to 2 substreams, 1 for writing row to parquet and 1 for sending metrics to
      // grafana
      final FanOutShape2<Pair<Row, List>, Row, List> UNZIP_ROW_METRIC =
          builder.add(Unzip.create(Row.class, List.class));

      final Integer DNS = Integer.valueOf(0);
      final Integer ICMP = Integer.valueOf(1);
      // create partitioner, to create a streams of dns packets and a streams of icmp packets
      // each stream will be sent to pool of sinks
      final UniformFanOutShape<Row, Row> PROT_PARTITIONER = builder
          .add(akka.stream.javadsl.Partition
              .create(Row.class, 2, e -> (e.getType() == ProtocolType.DNS) ? DNS : ICMP));

      // create sink for metrics
      SinkShape<List> metricSink = null;
      if (metricsEnabled) {
        metricSink = builder
            .add(Flow
                .of(List.class)
                // .buffer(bufferSize, OverflowStrategy.backpressure())
                .toMat(Sink.<List>foreach(r -> metricManager.update(r)), Keep.right())
                .async("metrics-dispatcher"));
      } else {
        // ignore metrics output
        metricSink = builder.add(Sink.<List>ignore());
      }

      // send outputs of unzipper to correct downstream operators
      builder.from(UNZIP_ROW_METRIC.out0()).viaFanOut(PROT_PARTITIONER);
      builder.from(UNZIP_ROW_METRIC.out1()).to(metricSink);

      // only add balancer to graph if dns and/or icmp writer pools >1 writer
      // having shorter graph is more efficient
      if (dnsWriterCount > 1) {
        final UniformFanOutShape<Row, Row> DNS_OUT = builder.add(Balance.create(dnsWriterCount));
        // connect partitioner to balancer
        builder.from(PROT_PARTITIONER.out(0)).viaFanOut(DNS_OUT);
        // connect the dns sinks to the corresponding balancer
        for (int i = 0; i < dnsWriterCount; i++) {
          // 1st get dns outs from list
          builder.from(DNS_OUT).to(outs.get(i));
        }
      } else {
        // single dns sink, do not use balancer
        builder.from(PROT_PARTITIONER.out(0)).to(outs.get(0));
      }

      if (icmpWriterCount > 1) {
        final UniformFanOutShape<Row, Row> ICMP_OUT = builder.add(Balance.create(icmpWriterCount));
        builder.from(PROT_PARTITIONER.out(1)).viaFanOut(ICMP_OUT);
        // connect the icmp sinks to the corresponding balancer
        for (int i = dnsWriterCount; i < outs.size(); i++) {
          // icmp outs are at end of the list after dns sinks
          builder.from(ICMP_OUT).to(outs.get(i));
        }
      } else {
        // single icmp sink, do not use balancer
        builder.from(PROT_PARTITIONER.out(1)).to(outs.get(dnsWriterCount));
      }

      // create graph flow
      builder.from(IN).via(DECODE).via(JOIN).via(BUILD_ROW).toInlet(UNZIP_ROW_METRIC.in());
      // return reusable graph
      return ClosedShape.getInstance();
    });
  }


  private void read(String file) {
    log.info("Start reading from file {}", file);

    // try to open file, if file is not good pcap handle exception and fail fast.
    if (!createReader(file)) {
      log.error("Skip bad input file: " + file);
      return;
    }

    try {
      // only create akka graph once
      if (graph == null) {
        graph = createGraph();
      }

      final List<CompletionStage<Done>> csList = RunnableGraph.fromGraph(graph).run(system);
      // wait until all graph sinks are done
      waitForGraphToComplete(csList);

      if (log.isDebugEnabled()) {
        log.debug("Done with pcap decoding");
      }
    } catch (Exception e) {
      // log all exception, should not happen
      log.error("Got exception from PCAP reader", e);
    } finally {
      // clear expired cache entries
      pcapReader.clearCache(cacheTimeoutTCPConfig * 1000, cacheTimeoutIPFragConfig * 60 * 1000);
      // make sure the pcap reader is always closed to avoid leaks
      pcapReader.close();
    }

  }

  private void waitForGraphToComplete(List<CompletionStage<Done>> csList) {
    for (CompletionStage<Done> cs : csList) {
      cs.toCompletableFuture().join();
    }
  }

  /**
   * Save the loader state with incomplete datagrams, tcp streams and unmatched dns queries to disk.
   */
  private void persistState() {
    log.info("Write internal state to file");

    int datagramCount = 0;
    int cacheCount = 0;
    long startTs = System.currentTimeMillis();

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

      if (joiner.getRequestCache() != null) {
        // persist request cache
        stateManager.write(joiner.getRequestCache());
        cacheCount = joiner.getRequestCache().size();
      }

      // flush metrics to make sure that metrics that can be sent already are sent
      // always send historical stats to monitoring
      if (metricsEnabled) {
        metricManager.persistState(stateManager);
      } else {
        // TODO: don't collect metrics if graphite is disabled, use micrometer abstraction directly
        // perhaps, to make it compatible with other metric backends?
        // At the moment this is not possible because micrometer does not allow sednding of
        // historical metrics through their api.
        // for now clear stats collection if graphite export is disabled, otherwise app goes OOM
        // after a while
        metricManager.clear();
      }

    } catch (Exception e) {
      log.error("Error writing state file", e);
      // delete old corrupt state
      stateManager.delete();
    } finally {
      stateManager.close();
    }

    log.info("------------- State Persist Stats ------------------------");
    log.info("Persist state to disk time {}", System.currentTimeMillis() - startTs);
    log.info("Persist {} TCP flows", flows.size());
    log.info("Persist {} Datagrams", datagramCount);
    log.info("Persist {} DNS requests from cache", cacheCount);
    log.info("Persist {} unsent metrics", metricManager.size());
    log.info("----------------------------------------------------------");
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
      if (flows == null) {
        flows = new HashMap<>();
      }

      // read persisted IP datagrams
      HashMap<Datagram, Collection<DatagramPayload>> inMap =
          (HashMap<Datagram, Collection<DatagramPayload>>) stateManager.read();
      if (inMap != null) {
        for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : inMap.entrySet()) {
          for (DatagramPayload dgPayload : entry.getValue()) {
            datagrams.put(entry.getKey(), dgPayload);
          }
        }
      }
      // read in previous request cache
      Map<RequestCacheKey, RequestCacheValue> requestCache =
          (Map<RequestCacheKey, RequestCacheValue>) stateManager.read();
      if (requestCache == null) {
        requestCache = new HashMap<>();
      }

      joiner.setRequestCache(requestCache);

      if (metricsEnabled) {
        metricManager.loadState(stateManager);
      }
    } catch (

    Exception e) {
      log.error("Error reading state file", e);
      // delete old corrupt state
      stateManager.delete();
    } finally {
      stateManager.close();
    }

    log.info("------------- Loader state stats -------------------------");
    log.info("Loaded TCP state {} TCP flows", flows.size());
    log.info("Loaded Datagram state {} Datagrams", datagrams.size());
    // log.info("Loaded Request cache {} DNS requests", packetJoiner.getRequestCache().size());
    log.info("Loaded metrics state {} metrics", metricManager.size());
    log.info("----------------------------------------------------------");
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
          CompressionUtil.getDecompressorStreamWrapper(ois.get(), bufferSizeConfig * 1024, file);
      this.pcapReader = new PcapReader(new DataInputStream(decompressor), ipDecoder, tcpEnabled,
          currentFileName, true);
    } catch (IOException e) {
      log.error("Error creating pcap reader for: " + file, e);
      try {
        ois.get().close();
      } catch (Exception e2) {
        log.error("Cannot close inputstream, maybe it was not yet opened");
      }
      return false;
    }

    // set the state of the reader, this can be loaded from disk and given
    // to the 1st reader. or it can be result of reader x and it is given to reader y
    // to have state continuity across readers.
    pcapReader.setTcpFlows(flows);
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
    List<String> files = fm.files(inputDir, false, ".pcap", ".pcap.gz", ".pcap.xz");

    files = files
        .stream()
        // sort asc by filename, name must include timestamp
        .sorted()
        // optionaly skip the newest file
        .limit(maxFiles(files))
        .collect(Collectors.toList());

    log.info("Found {} file(s) to process", files.size());

    if (log.isDebugEnabled()) {
      files.stream().forEach(file -> log.debug("Found file: {}", file));
    }
    return files;
  }

  private int maxFiles(List<String> files) {
    if (skipFirst) {
      if (files.size() > 0) {
        // skip newest file (sorted asc, newest file is the last in the list)
        return files.size() - 1;
      }
      // no files found
      return 0;
    }
    // return all files
    return files.size();
  }

}

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
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
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
import akka.stream.OverflowStrategy;
import akka.stream.SinkShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.GraphDSL.Builder;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Unzip;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.SharedContext;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.model.BaseMetricValues;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.ProtocolType;
import nl.sidnlabs.entrada.model.RowBuilder;
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
import nl.sidnlabs.pcap.packet.PacketFactory;
import nl.sidnlabs.pcap.packet.TCPFlow;

@Log4j2
@Component
// use prototype scope, create new bean each time batch of files is processed
// this to avoid problems with memory/caches when running app for a long period of time
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PacketProcessor {

  @Value("${entrada.tcp.enable:true}")
  private boolean tcpEnabled;

  @Value("${entrada.icmp.enable:true}")
  private boolean icmpEnabled;

  @Value("#{${entrada.cache.timeout.tcp.flows}*1000}")
  private int cacheTimeoutTCPConfig;

  @Value("#{${entrada.cache.timeout.ip.fragmented}*1000}")
  private int cacheTimeoutIPFragConfig;

  @Value("${entrada.inputstream.buffer:64}")
  private int bufferSizeConfig;

  @Value("${entrada.stream.buffer:16}")
  private int streamBufferSize;

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

  @Value("${entrada.row.builder.dns.count:1}")
  private int dnsRowbuilderCount;

  @Value("${entrada.row.builder.icmp.count:1}")
  private int icmpRowbuilderCount;

  @Value("${entrada.row.decoder.count:2}")
  private int rowDecoderCount;

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

  final Integer DNS = Integer.valueOf(0);
  final Integer ICMP = Integer.valueOf(1);
  final Integer UNKWN = Integer.valueOf(-1);

  private Config akkaConfig;
  private ActorSystem system;

  private Graph<ClosedShape, List<CompletionStage<Done>>> graph;
  private List<RowWriter> writers = new ArrayList<>();
  private PacketJoiner joiner;
  private List<IPDecoder> ipDecoders = new ArrayList<>();
  private RowBuilder dnsRowbuiler;
  private RowBuilder icmpRowbuiler;

  private boolean stateLoaded = false;

  public PacketProcessor(ServerContext serverCtx, StateManager persistenceManager,
      ArchiveService fileArchiveService, FileManagerFactory fileManagerFactory,
      PartitionService partitionService, HistoricalMetricManager historicalMetricManager,
      UploadService uploadService, SharedContext sharedContext, Config akkaConfig,
      ApplicationContext applicationContext, PacketJoiner joiner,
      @Qualifier("dns") RowBuilder dnsRowbuiler, @Qualifier("icmp") RowBuilder icmpRowbuiler) {

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
    this.joiner = joiner;
    this.dnsRowbuiler = dnsRowbuiler;
    this.icmpRowbuiler = icmpRowbuiler;
  }

  @PostConstruct
  private void init() {
    if (rowDecoderCount < 2) {
      throw new ApplicationException("Config option entrada.row.decoder.count must be >= 2");
    }
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
    ipDecoders.stream().forEach(IPDecoder::reset);
    dnsRowbuiler.reset();
    icmpRowbuiler.reset();
    joiner.reset();
  }

  public void execute() {
    try {
      execute_();
    } finally {
      // create new graph for each server
      stopAkka();
    }
  }

  private boolean isStateLoaded() {
    return stateLoaded;
  }

  private void execute_() {
    // search for input files
    List<String> inputFiles = scan();
    if (inputFiles.isEmpty()) {
      // no files found to process, stop
      return;
    }

    long partitionCheckTs = System.currentTimeMillis();
    long startAll = System.currentTimeMillis();
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

      // only create akka graph once
      if (graph == null) {
        graph = createGraph();
      }

      if (!isStateLoaded()) {
        // load the state from the previous run
        loadState();
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

      // move the pcap file to archive location or delete
      fileArchiveService.archive(file, startDate, joiner.getCounter(), fileSize);

      // reset counters
      reset();

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


    ipDecoders.stream().forEach(IPDecoder::printStats);
    dnsRowbuiler.printStats();
    icmpRowbuiler.printStats();
    writers.stream().forEach(RowWriter::printStats);
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


  private Integer protocol(Packet p) {
    // protocol can be zero for special NULL and LAST packets, send those to DNS streams
    if (p.getProtocol() == 0 || p.getProtocol() == PacketFactory.PROTOCOL_TCP
        || p.getProtocol() == PacketFactory.PROTOCOL_UDP) {
      return DNS;
    } else if (p.getProtocol() == PacketFactory.PROTOCOL_ICMP_V4
        || p.getProtocol() == PacketFactory.PROTOCOL_ICMP_V6) {
      return ICMP;
    }

    return UNKWN;
  }

  private UniformFanOutShape<Packet, Packet> createProtocolPartitioner(
      Builder<List<CompletionStage<Done>>> builder) {

    return builder
        .add(akka.stream.javadsl.Partition.create(Packet.class, 2, p -> this.protocol(p)));
  }

  private Graph<ClosedShape, List<CompletionStage<Done>>> createGraph() {

    final String serverName = serverCtx.getServerInfo().getServer();
    final List<Sink<GenericRecord, CompletionStage<Done>>> sinks = new ArrayList<>();

    // create sinks for dns
    for (int i = 0; i < dnsWriterCount; i++) {
      // parquet-dns writers are not thread safe, create new instances
      RowWriter w = applicationContext.getBean("parquet-dns", RowWriter.class);
      writers.add(w);

      Sink<GenericRecord, CompletionStage<Done>> s = Flow
          .of(GenericRecord.class)
          .buffer(streamBufferSize, OverflowStrategy.backpressure())
          .toMat(Sink.<GenericRecord>foreach(r -> w.write(r, serverName)), Keep.right())
          .async("entrada-dispatcher");

      sinks.add(s);
    }

    // create sinks for icmp
    for (int i = 0; i < icmpWriterCount; i++) {

      RowWriter w = applicationContext.getBean("parquet-icmp", RowWriter.class);
      writers.add(w);

      Sink<GenericRecord, CompletionStage<Done>> s = Flow
          .of(GenericRecord.class)
          .buffer(streamBufferSize, OverflowStrategy.backpressure())
          .toMat(Sink.<GenericRecord>foreach(r -> w.write(r, serverName)), Keep.right())
          .async("entrada-dispatcher");

      sinks.add(s);
    }

    // create reuseable akka streams graph
    return GraphDSL.create(sinks, (builder, outs) -> {

      final Executor ex = system.dispatchers().lookup("entrada-dispatcher");

      // create sink for metrics
      final SinkShape<BaseMetricValues> metricSink = createMetricsSink(builder);
      final UniformFanInShape<BaseMetricValues, BaseMetricValues> METRIC_SINK_MERGE =
          builder.add(Merge.create(2));
      builder.from(METRIC_SINK_MERGE).to(metricSink);

      // create input source, reading pcap file on dedicated thread
      final Outlet<Packet> IN = builder
          .add(Source
              .fromJavaStream(() -> pcapReader.stream())
              .buffer(streamBufferSize, OverflowStrategy.backpressure())
              // send last packet when pcap stream is done to let downstream operators
              // cleanup/close
              .concat(Source.single(Packet.LAST))
              .async("entrada-dispatcher")
              .log("Akka-SRC"))
          .out();


      // create operators for decoding the packets from pcap bytes
      final List<FlowShape<Packet, Packet>> DECODERS = createDecoders(builder);
      final UniformFanInShape<Packet, Packet> MERGE_DECODERS =
          builder.add(Merge.create(rowDecoderCount));

      // create partitioner, to create multiple 2 streams 1 for dns and 1 for icmp
      final UniformFanOutShape<Packet, Packet> DECODER_PARTITIONER =
          createDecoderPartitioner(builder);

      builder.from(IN).toFanOut(DECODER_PARTITIONER);
      // link each decoder to the upstream decoder partitioner and the downstream merge
      for (int i = 0; i < DECODERS.size(); i++) {
        builder.from(DECODER_PARTITIONER.out(i)).via(DECODERS.get(i)).viaFanIn(MERGE_DECODERS);
      }

      // create partition to split stream based on protocol type
      final UniformFanOutShape<Packet, Packet> PROT_PARTITIONER =
          createProtocolPartitioner(builder);

      // Create ICMP flow
      final FlowShape<Packet, Pair<GenericRecord, BaseMetricValues>> ICMP_ROW_BUILDER = builder
          .add(Flow
              .of(Packet.class)
              .buffer(streamBufferSize, OverflowStrategy.backpressure())
              .mapAsyncUnordered(icmpRowbuilderCount, p -> CompletableFuture
                  .supplyAsync(() -> icmpRowbuiler.build(p, serverName), ex)));

      // unzipper for row-builder output
      final FanOutShape2<Pair<GenericRecord, BaseMetricValues>, GenericRecord, BaseMetricValues> ICMP_UNZIP_ROW_METRIC =
          builder.add(Unzip.create(GenericRecord.class, BaseMetricValues.class));

      // balancer for icmp writers
      final UniformFanOutShape<GenericRecord, GenericRecord> ICMP_OUT_BALANCER =
          builder.add(Balance.create(icmpWriterCount));

      for (int i = dnsWriterCount; i < outs.size(); i++) {
        // icmp outs are at end of the list after dns sinks
        builder.from(ICMP_OUT_BALANCER).to(outs.get(i));
      }

      // connect everything
      // builder -> unzipper
      builder.from(ICMP_ROW_BUILDER).toInlet(ICMP_UNZIP_ROW_METRIC.in());
      // unzipper packet -> writer balancer
      builder.from(ICMP_UNZIP_ROW_METRIC.out0()).viaFanOut(ICMP_OUT_BALANCER);
      // unzipper packet -> metric merge
      builder.from(ICMP_UNZIP_ROW_METRIC.out1()).toFanIn(METRIC_SINK_MERGE);
      // protocol partitioner -> icmp row builder
      builder.from(PROT_PARTITIONER.out(ICMP.intValue())).via(ICMP_ROW_BUILDER);
      // out from decoder merge to protocol partitioner
      builder.from(MERGE_DECODERS).viaFanOut(PROT_PARTITIONER);

      // Now add DNS flow

      // add step to join packets (this is not thread safe, don't run this parallel
      final FlowShape<Packet, RowData> JOIN = builder
          .add(Flow
              .of(Packet.class)
              .buffer(streamBufferSize, OverflowStrategy.backpressure())
              .mapConcat(p -> joiner.join(p)));

      // from protocol partioner send dns packets to joiner
      builder.from(PROT_PARTITIONER.out(DNS.intValue())).via(JOIN);

      // create operator to build rows
      final FlowShape<RowData, Pair<GenericRecord, BaseMetricValues>> DNS_ROW_BUILDER = builder
          .add(Flow
              .of(RowData.class)
              .buffer(streamBufferSize, OverflowStrategy.backpressure()) // .withLogLevel(Logging.InfoLevel()))
              .mapAsyncUnordered(dnsRowbuilderCount, rd -> CompletableFuture
                  .supplyAsync(() -> dnsRowbuiler.build(rd, serverName), ex)));


      // from dns joiner -> row builder
      builder.from(JOIN).via(DNS_ROW_BUILDER);

      // unzip, split row and metric into 2 separate streams
      final FanOutShape2<Pair<GenericRecord, BaseMetricValues>, GenericRecord, BaseMetricValues> DNS_UNZIP_ROW_METRIC =
          builder.add(Unzip.create(GenericRecord.class, BaseMetricValues.class));

      // output from row builder to unzipper
      builder.from(DNS_ROW_BUILDER).toInlet(DNS_UNZIP_ROW_METRIC.in());

      // create balancer for dns writers
      final UniformFanOutShape<GenericRecord, GenericRecord> DNS_WRITER_BALANCER =
          builder.add(Balance.create(dnsWriterCount));

      // link all balancer ports to dns sinks
      for (int i = 0; i < dnsWriterCount; i++) {
        // 1st get dns outs from list
        builder.from(DNS_WRITER_BALANCER).to(outs.get(i));
      }

      // unzipper dns row -> writer balancer
      builder.from(DNS_UNZIP_ROW_METRIC.out0()).viaFanOut(DNS_WRITER_BALANCER);
      // unzipper metric -> metric merge
      builder.from(DNS_UNZIP_ROW_METRIC.out1()).toFanIn(METRIC_SINK_MERGE);

      return ClosedShape.getInstance();
    });
  }

  private List<FlowShape<Packet, Packet>> createDecoders(
      Builder<List<CompletionStage<Done>>> builder) {

    final Executor ex = system.dispatchers().lookup("entrada-dispatcher");

    List<FlowShape<Packet, Packet>> decoders = new ArrayList<>();
    for (int i = 0; i < rowDecoderCount; i++) {

      // create deocder here, so we can offload the decoding to a separate thread
      TCPDecoder tcpDecoder = null;
      // can share decoders, decoding is single threaded in akka streams
      DNSDecoder dnsDecoder = new DNSDecoder(false);
      if (tcpEnabled) {
        tcpDecoder = new TCPDecoder(dnsDecoder);
      }
      IPDecoder ipDecoder =
          new IPDecoder(tcpDecoder, new UDPDecoder(dnsDecoder), new ICMPDecoder());

      ipDecoders.add(ipDecoder);

      FlowShape<Packet, Packet> d = builder
          .add(Flow
              .of(Packet.class)
              .buffer(streamBufferSize, OverflowStrategy.backpressure())
              .mapAsyncUnordered(1,
                  p -> CompletableFuture.supplyAsync(() -> ipDecoder.decode(p), ex)));

      decoders.add(d);

    }

    return decoders;
  }

  /**
   * The same src/dst and dst/src combinations must be sent to the same decoder because decoders are
   * stateful
   * 
   * @param p
   * @return
   */
  private Integer decoderPartitionerSelector(Packet p) {
    if (p.getSrcAddr() == null) {
      return Integer.valueOf(0);
    }

    int hash = Math.abs(p.getSrcAddr().hashCode() - p.getDstAddr().hashCode());
    return Integer.valueOf(hash % rowDecoderCount);
  }

  private UniformFanOutShape<Packet, Packet> createDecoderPartitioner(
      Builder<List<CompletionStage<Done>>> builder) {

    if (rowDecoderCount > 1) {
      return builder
          .add(akka.stream.javadsl.Partition
              .create(Packet.class, rowDecoderCount, p -> decoderPartitionerSelector(p)));
    }
    return null;

  }

  private SinkShape<BaseMetricValues> createMetricsSink(
      Builder<List<CompletionStage<Done>>> builder) {
    if (metricsEnabled) {
      return builder
          .add(Flow
              .of(BaseMetricValues.class)
              .buffer(streamBufferSize, OverflowStrategy.backpressure())
              .toMat(Sink.<BaseMetricValues>foreach(r -> metricManager.update(r)), Keep.right())
              .async("entrada-dispatcher"));
    }
    // ignore metrics output
    return builder.add(Sink.<BaseMetricValues>ignore());

  }

  private void read(String file) {
    log.info("Start reading from file {}", file);

    // try to open file, if file is not good pcap handle exception and fail fast.
    if (!createReader(file)) {
      log.error("Skip bad input file: " + file);
      return;
    }

    try {

      final List<CompletionStage<Done>> csList = RunnableGraph.fromGraph(graph).run(system);
      // wait until all graph sinks are done
      waitForGraphToComplete(csList);

      if (log.isDebugEnabled()) {
        log.debug("Done executing Akka Streams graph");
      }
    } catch (Exception e) {
      // log all exception, should not happen
      log.error("Got exception running kka Streams graph", e);
    } finally {
      // clear expired cache entries
      pcapReader.clearCache(cacheTimeoutTCPConfig, cacheTimeoutIPFragConfig);
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
    int flowCount = 0;
    int cacheCount = 0;
    long startTs = System.currentTimeMillis();

    try {
      // persist tcp state
      for (IPDecoder id : ipDecoders) {
        Map<TCPFlow, FlowData> flows = ((TCPDecoder) id.getTcpReader()).getFlows();
        flowCount += flows.size();
        stateManager.writeObject(flows);
      }

      for (IPDecoder id : ipDecoders) {

        if (id.getDatagrams() != null) {
          // persist IP datagrams
          Map<Datagram, Collection<DatagramPayload>> outMap = new HashMap<>();
          for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : id
              .getDatagrams()
              .asMap()
              .entrySet()) {
            Collection<DatagramPayload> datagrams2persist = new ArrayList<>();
            for (DatagramPayload sequencePayload : entry.getValue()) {
              datagrams2persist.add(sequencePayload);
            }
            outMap.put(entry.getKey(), datagrams2persist);
            datagramCount++;
          }

          stateManager.writeObject(outMap);
        }
      }

      if (joiner.getRequestCache() != null) {
        // persist request cache
        stateManager.writeObject(joiner.getRequestCache());
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
    log.info("{} (ms) save to disk time", System.currentTimeMillis() - startTs);
    log.info("{} TCP flows", flowCount);
    log.info("{} UDP datagrams", datagramCount);
    log.info("{} DNS requests from joiner cache", cacheCount);
    log.info("{} unsent metrics", metricManager.size());
    log.info("----------------------------------------------------------");
  }

  @SuppressWarnings("unchecked")
  private void loadState() {
    log.info("Load internal state from file");

    stateLoaded = true;
    int flowCount = 0;
    int datagramCount = 0;
    try {
      if (!stateManager.stateAvailable()) {
        log.info("No state file found, do not try to load previous state");

        // tcpFlows = new HashMap<>();
        return;
      }
      for (IPDecoder id : ipDecoders) {
        // read persisted TCP sessions
        Map<TCPFlow, FlowData> flows = (Map<TCPFlow, FlowData>) stateManager.readObject();
        if (flows != null) {
          flowCount += flows.size();
          ((TCPDecoder) id.getTcpReader()).setFlows(flows);
        }
      }

      for (IPDecoder id : ipDecoders) {
        // read persisted IP datagrams
        HashMap<Datagram, Collection<DatagramPayload>> inMap =
            (HashMap<Datagram, Collection<DatagramPayload>>) stateManager.readObject();
        if (inMap != null) {
          Multimap<Datagram, DatagramPayload> datagrams = id.getDatagrams();
          for (Map.Entry<Datagram, Collection<DatagramPayload>> entry : inMap.entrySet()) {
            for (DatagramPayload dgPayload : entry.getValue()) {
              datagrams.put(entry.getKey(), dgPayload);
            }
          }
          datagramCount += datagrams.size();
        }
      }

      // read in previous request cache
      Map<RequestCacheKey, RequestCacheValue> requestCache =
          (Map<RequestCacheKey, RequestCacheValue>) stateManager.readObject();
      if (requestCache == null) {
        requestCache = new HashMap<>();
      }

      joiner.setRequestCache(requestCache);

      if (metricsEnabled) {
        metricManager.loadState(stateManager);
      }
    } catch (Exception e) {
      // when changing the number of configured decoders, loading the old state will fail
      log.error("Error reading state file", e);
      // delete old corrupt state
      stateManager.delete();

      for (IPDecoder id : ipDecoders) {
        ((TCPDecoder) id.getTcpReader()).setFlows(new HashMap<>());
        id.setDatagrams(TreeMultimap.create());
      }
      joiner.setRequestCache(new HashMap<>());
      metricManager.clear();
    } finally {
      stateManager.close();
    }

    log.info("------------- State loading Stats -------------------------");
    log.info("Loaded TCP state {} TCP flows", flowCount);
    log.info("Loaded Datagram state {} Datagrams", datagramCount);
    log.info("Loaded Request cache {} DNS requests", joiner.getRequestCache().size());
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

    IPDecoder ipDecoder = new IPDecoder(null, null, null);

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

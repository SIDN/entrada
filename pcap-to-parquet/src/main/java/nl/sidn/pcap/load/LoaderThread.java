/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].
 *
 */	
package nl.sidn.pcap.load;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.types.MessageType;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.SequencePayload;
import nl.sidn.pcap.decoder.ICMPDecoder;
import nl.sidn.pcap.packet.DNSPacket;
import nl.sidn.pcap.packet.Datagram;
import nl.sidn.pcap.packet.DatagramPayload;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.packet.TCPFlow;
import nl.sidn.pcap.support.AbstractStoppableThread;
import nl.sidn.pcap.support.MessageWrapper;
import nl.sidn.pcap.support.PacketCombination;
import nl.sidn.pcap.support.RequestKey;
import nl.sidn.pcap.util.Settings;
import nl.sidn.pcap.util.Settings.ServerInfo;
import nl.sidn.stats.MetricManager;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

public class LoaderThread extends AbstractStoppableThread {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(LoaderThread.class);
	
	private static String DECODER_STATE_FILE = "pcap-decoder-state";
	
	private PcapReader pcapReader;
	
	protected Map<RequestKey, MessageWrapper> _requestCache = new HashMap<RequestKey, MessageWrapper>();

	private BlockingQueue<PacketCombination> sharedQueue;
	
	private List<String> processedFiles = new ArrayList<String>();
	private List<String> inputFiles = new ArrayList<String>();;
	
	private String inputDir;
	private String outputDir;
	private String stateDir;

	private DataInputStream dis;

	private int multiCounter;
	private ServerInfo current_server = Settings.getInstance().getServer();
	
	//metrics
	private int queryCounter;
	private int responseCounter;
	private int fileCount = 0;
	private int purgeCounter = 0;
	//counter when no request query can be found for a response
	private int noQueryFoundCounter = 0;
	
	//max lifetime for packets
	//NOTE: the packets are processed in batches 1 batch (pcap file) per 5 minutes
	//this will cause the the packets to timeout every 2nd pcap file.
	//this should not be a problem as it is not expected that a
	//packet from pcap 1 will get a response in pcap 3
	private int cacheTimeout;

	public LoaderThread(BlockingQueue<PacketCombination> sharedQueue, String inputDir, String outputDir, String stateDir){
		this.sharedQueue = sharedQueue;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.stateDir = stateDir;
		cacheTimeout = 1000 * 60 * Integer.parseInt(Settings.getInstance().getSetting(Settings.CACHE_TIMEOUT));
		pcapReader = new PcapReader();
	}

	@Override
	protected void doWork() {
		//search for input files
		scan();
		if(filesEmpty()){
			//no files found to process, stop
			LOGGER.info("No files found, stop.");
			//add marker packet indicating all packets are decoded
			sharedQueue.add(PacketCombination.NULL);
			return;
		}
		//get the state from the previous run
		loadState();
		for (String file : inputFiles) {
			read(file);
			//flush expired packets after very file, avoid huge cache
			purgeCache();
			waitForEmptyQueue();
			archiveFile(file);
		}
		//save unmatched packet state to file
		//the next pcap might have the missing responses
		persistState();
		
		//add marker packet indicating all packets are decoded
		sharedQueue.add(PacketCombination.NULL);
		
		LOGGER.info("--------- Done loading queue ------------");
		LOGGER.info("Loaded " + (queryCounter+responseCounter) + " packets");
		LOGGER.info("Loaded " + queryCounter + " query packets");
		LOGGER.info("Loaded " + responseCounter + " response packets");
		LOGGER.info("Loaded " + multiCounter + " messages from TCP streams with > 1 mesg");
		LOGGER.info("Found " + noQueryFoundCounter + " response packets without request.");
		LOGGER.info("-----------------------------------------");
		
		writeMetrics();
	}

	/**
	 * Write the loader metrics to the metrics queue
	 */
	private void writeMetrics() {
		MetricManager mm = MetricManager.getInstance();
		mm.send(MetricManager.METRIC_IMPORT_FILES_COUNT, fileCount);
		mm.send(MetricManager.METRIC_IMPORT_DNS_NO_REQUEST_COUNT, noQueryFoundCounter);
		mm.send(MetricManager.METRIC_IMPORT_DNS_TCPSTREAM_COUNT, multiCounter);
		mm.send(MetricManager.METRIC_IMPORT_STATE_PERSIST_UDP_FLOW_COUNT, pcapReader.getDatagrams().size());
		mm.send(MetricManager.METRIC_IMPORT_STATE_PERSIST_TCP_FLOW_COUNT, pcapReader.getFlows().size());
		mm.send(MetricManager.METRIC_IMPORT_STATE_PERSIST_DNS_COUNT, _requestCache.size());
		mm.send(MetricManager.METRIC_IMPORT_TCP_PREFIX_ERROR_COUNT, pcapReader.getTcpPrefixError());
		mm.send(MetricManager.METRIC_IMPORT_DNS_DECODE_ERROR_COUNT, pcapReader.getDnsDecodeError());
	}

	private boolean filesEmpty() {
		return inputFiles.size() == 0;
	}

	private void archiveFile(String pcap) {
		File file = new File(pcap);
		File archiveDir = new File(outputDir + System.getProperty("file.separator") + "archive"  + System.getProperty("file.separator") + current_server.getFullname());
		if(!archiveDir.exists()){
			LOGGER.info(archiveDir.getName() + " does not exist, create now.");
			if(!archiveDir.mkdirs()){
				throw new RuntimeException("creating archive dir: " + archiveDir.getAbsolutePath());
			}
		}
		 if(file.renameTo(new File(archiveDir.getPath() + System.getProperty("file.separator") + file.getName()))){
			LOGGER.info(file.getName() + " is archived!");
		}else{
			throw new RuntimeException("Error moving " + file.getName() + " to the archive");
		}
	}

	/**
	 * Avoid filling the queue with too many packets
	 * this will fill up the heap space and cause
	 * endless GC
	 */
	private void waitForEmptyQueue() {
		while(sharedQueue.size() > 0){
			LOGGER.info("Shared queue not empty sleep for 5s");
			try {
				Thread.sleep(5*1000);
			} catch (InterruptedException e) {
				LOGGER.info("Interrupted while sleeping");
			}
		}
		
		LOGGER.info("Shared queue is empty continue with next file");
		
	}

	public void read(String file) {
		createReader(file);
		LOGGER.info("Start loading queue");
		long counter = 0;
		for (Packet currentPacket : pcapReader) {
			counter++;
			if(counter % 100000 == 0){
				LOGGER.info("Loaded " + counter + " packets");
			}
			if(currentPacket != null && currentPacket.getIpVersion() != 0){
				
				if((currentPacket.getProtocol() == ICMPDecoder.PROTOCOL_ICMP_V4) ||
						(currentPacket.getProtocol() == ICMPDecoder.PROTOCOL_ICMP_V6)){
					//handle icmp
					PacketCombination pc = new PacketCombination(currentPacket, null, current_server);
					
					try {
						if(!sharedQueue.offer(pc , 5, TimeUnit.SECONDS)){
							LOGGER.error("timeout adding items to queue");
						}
					} catch (InterruptedException e) {
						LOGGER.error("interrupted while adding ICMP item to queue");
					}
			
				}else{
					DNSPacket dnsPacket = (DNSPacket)currentPacket;
					if(dnsPacket.getMessage() == null){
						//skip malformed packets
						LOGGER.debug("loader skipping packet with no dns message");
						continue;
					}
					
					if(dnsPacket.getMessageCount() > 1){
						multiCounter = multiCounter + dnsPacket.getMessageCount();
					}
					
					for (Message msg : dnsPacket.getMessages()) {
						//get qname from request which is part of the cache lookup key
						String qname = null;
				    	if(msg != null && msg.getQuestions() != null && msg.getQuestions().size() > 0){
				    		qname = msg.getQuestions().get(0).getqName();
				    	}
						//put request into cache with a key based on: query id, qname, ip src, tcp/udp port 
						//add time for possible timeout eviction
						if(msg.getHeader().getQr() == MessageType.QUERY){  
							queryCounter++;
							RequestKey key = new RequestKey(msg.getHeader().getId(), qname, dnsPacket.getSrc(), dnsPacket.getSrcPort(), System.currentTimeMillis());
							_requestCache.put(key, new MessageWrapper(msg,dnsPacket));
						}else{
							//try to find the request
							responseCounter++;
							RequestKey key = null;
		
							key = new RequestKey(msg.getHeader().getId(),qname, dnsPacket.getDst(), dnsPacket.getDstPort());
							MessageWrapper request = _requestCache.remove(key);
							//check to see if the request packet exists, at the start of the pcap there may be missing querys
											
							if(request != null &&  request.getPacket() != null && request.getMessage() != null){
								try {
									if(!sharedQueue.offer(new PacketCombination(request.getPacket(), request.getMessage(), current_server, dnsPacket, msg) , 5, TimeUnit.SECONDS)){
										LOGGER.error("timeout adding items to queue");
									}
								} catch (InterruptedException e) {
									LOGGER.error("interrupted while adding items to queue");
								}
							}else{
								//no request found, this could happen if the query was in previous pcap 
								//and was not correctly decoded, or the request timed out before server
								//could send a response.
								LOGGER.debug("Found no request for response");
								noQueryFoundCounter++;
								
								try {
									if(!sharedQueue.offer(new PacketCombination(null, null, current_server, dnsPacket,msg ), 5, TimeUnit.SECONDS)){
										LOGGER.error("timeout adding items to queue");
									}
									purgeCounter++;
								} catch (InterruptedException e) {
									LOGGER.error("interrupted while adding items to queue");
								}
							}
						}
					}
				} //end of dns packet
			}
		}
		if(LOGGER.isDebugEnabled()){
			LOGGER.debug("Done with decoding, start cleanup");
		}
		//clear expired cache entries
		pcapReader.clearCache();
		pcapReader.close();
	}

	private String createStateFileName(){
		return stateDir + "/" + DECODER_STATE_FILE + "-" + current_server.getFullname() + ".bin";
	}
	
	/**
	 * Save the loader state with incomplete datagrams, tcp streams and unmatched dns queries to disk.
	 */
	private void persistState(){
		 Kryo kryo = new Kryo();
		 Output output = null;
		 String file = createStateFileName();
		try {
			//persist tcp state
			output = new Output(new FileOutputStream(file));		
			Map<TCPFlow, Collection<SequencePayload>> flows = pcapReader.getFlows().asMap();
			//convert to std java map and collection
			Map<TCPFlow, Collection<SequencePayload>> pmap = new HashMap<TCPFlow, Collection<SequencePayload>>();
			Iterator<TCPFlow> iter = flows.keySet().iterator();
			while (iter.hasNext()) {
				TCPFlow tcpFlow = (TCPFlow) iter.next();
				Collection<SequencePayload> payloads = new ArrayList<SequencePayload>();
				Collection<SequencePayload> payloads2Persist = flows.get(tcpFlow);
				for (SequencePayload sequencePayload : payloads2Persist) {
					payloads.add(sequencePayload);
				}
				pmap.put(tcpFlow, payloads);
			}
			kryo.writeObject(output, pmap);

			//persist IP datagrams
			Map<Datagram, Collection<DatagramPayload>> datagrams = pcapReader.getDatagrams().asMap();
			//convert to std java map and collection
			Map<Datagram, Collection<DatagramPayload>> outMap = new HashMap<Datagram, Collection<DatagramPayload>>();
			Iterator<Datagram> ipIter = datagrams.keySet().iterator();
			while (iter.hasNext()) {
				Datagram dg = (Datagram) ipIter.next();
				Collection<DatagramPayload> datagrams2persist = new ArrayList<DatagramPayload>();
				Collection<DatagramPayload> datagramPayloads = datagrams.get(dg);
				for (DatagramPayload sequencePayload : datagramPayloads) {
					datagrams2persist.add(sequencePayload);
				}
				outMap.put(dg, datagrams2persist);
			}
			
			kryo.writeObject(output, outMap);
			
			//persist request cache
			kryo.writeObject(output, _requestCache);
			
			output.close();
			LOGGER.info("------------- State persistence stats --------------");
			LOGGER.info("Data is persisted to " + file);
			LOGGER.info("Persist " + pmap.size() + " TCP flows");
			LOGGER.info("Persist " + pcapReader.getDatagrams().size() + " Datagrams");
		    LOGGER.info("Persist request cache " + _requestCache.size() + " DNS requests");
		    LOGGER.info("----------------------------------------------------");
			
		} catch (Exception e) {
			LOGGER.error("Error saving decoder state to file: " + file, e);
		}
		
	}
	
	private void loadState(){
		Kryo kryo = new Kryo();
		String file = createStateFileName();
		if(!Files.exists(Paths.get(file))){
			LOGGER.info("No state found at " + file);
			return;
		}
		try {
			 Input input = new Input(new FileInputStream(file));
			 
			 //read persisted TCP sessions
			 Multimap<TCPFlow, SequencePayload> flows = TreeMultimap.create();
			 HashMap<TCPFlow, Collection<SequencePayload>> map = kryo.readObject(input, HashMap.class);
			 for (TCPFlow flow : map.keySet()) {
				 Collection<SequencePayload> payloads = map.get(flow);
				 for (SequencePayload sequencePayload : payloads) {
					 flows.put(flow, sequencePayload);
				}
			 }			 
			 pcapReader.setFlows(flows);

			 //read persisted IP datagrams
			 Multimap<nl.sidn.pcap.packet.Datagram, nl.sidn.pcap.packet.DatagramPayload> datagrams = TreeMultimap.create();
			 HashMap<Datagram, Collection<DatagramPayload>> inMap = kryo.readObject(input, HashMap.class);
			 for (Datagram flow : inMap.keySet()) {
				 Collection<DatagramPayload> payloads = inMap.get(flow);
				 for (DatagramPayload dgPayload : payloads) {
					 datagrams.put(flow, dgPayload);
				}
			 }			 
			 pcapReader.setDatagrams(datagrams);

			 //read in previous request cache
			 _requestCache = kryo.readObject(input, HashMap.class);
		     input.close();
		     LOGGER.info("------------- Loader state stats ------------------");
		     LOGGER.info("Loaded TCP state " + pcapReader.getFlows().size() + " TCP flows");
		     LOGGER.info("Loaded Datagram state " + pcapReader.getDatagrams().size() + " Datagrams");
		     LOGGER.info("Loaded Request cache " + _requestCache.size() + " DNS requests");
		     LOGGER.info("----------------------------------------------------");
		} catch (Exception e) {
			LOGGER.error("Error opening state file, continue without loading state: " + file, e);
		}
		
	}

	
	private void purgeCache() {
		//remove expired entries from _requestCache
		Iterator<RequestKey> iter = _requestCache.keySet().iterator();
		long now = System.currentTimeMillis();
		
		while(iter.hasNext()){
			RequestKey key = iter.next();
			if(key.getTime() < now-cacheTimeout){
				//remove expired request;
				MessageWrapper mw =_requestCache.get(key);
				iter.remove();				
				  
				if(mw.getMessage() != null && mw.getMessage().getHeader().getQr() == MessageType.QUERY){
					try {
						if(!sharedQueue.offer(new PacketCombination(mw.getPacket(), mw.getMessage(), current_server), 5, TimeUnit.SECONDS)){
							LOGGER.error("timeout adding items to queue");
						}
						purgeCounter++;
					} catch (InterruptedException e) {
						LOGGER.error("interrupted while adding items to queue");
					}
				}else{
					LOGGER.debug("Cached response entry timed out, request might have been missed");
					noQueryFoundCounter++;
				}
			}
		}
		
		LOGGER.info("Marked " + purgeCounter + " expired queries from request cache to output file with rcode no response");
	}

	public void createReader(String file) {
		LOGGER.info("Start loading queue from file:" + file);
		fileCount++;
		try {
			File f = FileUtils.getFile(file);
			LOGGER.info("Load data for server: " + current_server.getFullname());
			   
		    FileInputStream fis = FileUtils.openInputStream(f);
			GZIPInputStream gzip = new GZIPInputStream(fis);
		    dis = new DataInputStream(gzip);
			pcapReader.init(dis);
		} catch (IOException e) {
			LOGGER.error("Error opening pcap file: " + file, e);
			throw new RuntimeException("Error opening pcap file: " + file);
		}
	}
	
	private void scan() {
		LOGGER.info("Scan for pcap files in: " + inputDir);
		File f = null;
		try {
			f = FileUtils.getFile(inputDir);
		} catch (Exception e) {
			throw new RuntimeException("input dir error",e);
		}
		Iterator<File> files = FileUtils.iterateFiles(f, new String[]{"pcap.gz"}, false);
	    while(files.hasNext()) {
	    	File pcap = files.next();
			String fqn = pcap.getAbsolutePath();
			if(!processedFiles.contains(fqn) && !inputFiles.contains(fqn) ){
				inputFiles.add(fqn);
			}
	    }
	    //sort the files by name, tcp streams and udp fragmentation may overlap multiple files.
	    //so ordering is important.
	    Collections.sort(inputFiles);
	    for (String file : inputFiles) {
	    	LOGGER.info("Found file: " + file);
		}
	}

}

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
package nl.sidn.pcap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import nl.sidn.pcap.decoder.ICMPDecoder;
import nl.sidn.pcap.load.LoaderThread;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.parquet.DNSParquetPacketWriter;
import nl.sidn.pcap.parquet.ICMPParquetPacketWriter;
import nl.sidn.pcap.support.NamedThreadFactory;
import nl.sidn.pcap.support.PacketCombination;
import nl.sidn.pcap.support.RequestKey;
import nl.sidn.pcap.util.Settings;
import nl.sidn.stats.MetricManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller {
	private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);
	
	private ExecutorService executor;
    protected final Map<RequestKey, Packet> _requestCache = new HashMap<RequestKey, Packet>();
	private BlockingQueue<PacketCombination> sharedQueue = new LinkedBlockingQueue<PacketCombination>();
	
	//default max 3 mil packets per files max (+/- 125mb files)
	private int parquetMaxPackets = 3000000; 
	private int counter;
	private int totalCounter;

	private int icmpCounter;
	private int icmpTotalCounter;
	
	//private PcapLoader ploader;
	private DNSParquetPacketWriter dnsWriter;
	private ICMPParquetPacketWriter icmpWriter;
	
	//metric counters
	int udp = 0;
	int tcp = 0;
	int icmp = 0;
	
	public Controller(){
		Settings setting = Settings.getInstance();
		/* create a pcap loader, reading and decoding pcap files is done
		 * by this separate thread. This allows for simultaneous writing
		 * of decoded packets to parquet
		 */
		startLoaderThread(setting.getSetting(Settings.INPUT_LOCATION) + System.getProperty("file.separator") + Settings.getInstance().getServer().getFullname(),
				setting.getSetting(Settings.STATE_LOCATION),setting.getSetting(Settings.OUTPUT_LOCATION));
		
		if(setting.getSetting(Settings.OUTPUT_MAX_PACKETS) != null){
			parquetMaxPackets = Integer.parseInt(setting.getSetting(Settings.OUTPUT_MAX_PACKETS));
		}

		dnsWriter = new DNSParquetPacketWriter("dnsdata","dns-query.avsc");
		dnsWriter.open();
		icmpWriter = new ICMPParquetPacketWriter("icmpdata","icmp-packet.avsc");
		icmpWriter.open();
	}
	
	public void startLoaderThread(String inputDir, String stateDir, String outputDir){
		LOGGER.info("Start LoaderThread");
		executor = Executors.newFixedThreadPool(1, new NamedThreadFactory("PCAPLoader-Thread"));
		LoaderThread ld = new LoaderThread(sharedQueue,inputDir, outputDir, stateDir);
		executor.submit(ld);
	}
	
	public void start(){	
		PacketCombination p = null;
		do{
			//read packets from the shared queue
			p = nextPacket();
			
			if(p != null && p != PacketCombination.NULL){
				int proto = lookupProtocol(p);
				if(proto == PcapReader.PROTOCOL_TCP){
					tcp++; 
					writeDns(p);
				}else if(proto == PcapReader.PROTOCOL_UDP){
					udp++;
					writeDns(p);
				}else if(proto == ICMPDecoder.PROTOCOL_ICMP_V4 || proto == ICMPDecoder.PROTOCOL_ICMP_V6){
					icmp++;
					writeIcmp(p);
				}
			}
		}while(p != PacketCombination.NULL);
		
		LOGGER.info("processed " + totalCounter + " DNS packets." );
		LOGGER.info("processed " + icmpTotalCounter + " ICMP packets." );
		MetricManager metricManager = MetricManager.getInstance();
		metricManager.send(MetricManager.METRIC_IMPORT_DNS_COUNT, totalCounter);
		metricManager.send(MetricManager.METRIC_ICMP_COUNT, icmpTotalCounter);
		metricManager.send(MetricManager.METRIC_IMPORT_TCP_COUNT, tcp);
		metricManager.send(MetricManager.METRIC_IMPORT_UDP_COUNT, udp);
		dnsWriter.writeMetrics();
		icmpWriter.writeMetrics();
	}
	
	/**
	 * Lookup protocol, if no request is found then get the proto from the response.
	 * @param p
	 * @return
	 */
	private int lookupProtocol(PacketCombination p){
		if(p.getRequest() != null){
			return p.getRequest().getProtocol();
		}else if(p.getResponse() != null){
			return p.getResponse().getProtocol();
		}
		
		//unknown proto
		return -1;
	}
	
	public PacketCombination nextPacket() {
		PacketCombination p;
		try {
			p = sharedQueue.poll(3, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Error waiting for dns packets");
		}
		return p;
	}
	
	
	private void writeDns(PacketCombination p){
		dnsWriter.write(p);
		counter++;
		totalCounter++;
		if(counter >= parquetMaxPackets){
			dnsWriter.close();
			//create new writer
			dnsWriter.open();
			//reset counter
			counter = 0;
		}
	}
	
	private void writeIcmp(PacketCombination p){
		icmpWriter.write(p);
		icmpCounter++;
		icmpTotalCounter++;
		if(icmpCounter >= parquetMaxPackets){
			icmpWriter.close();
			//create new writer
			icmpWriter.open();
			//reset counter
			icmpCounter = 0;
		}
	}
	
	public void close(){
		LOGGER.info("Stop executor for LoaderThread");
		executor.shutdownNow();
		//make sure to close last partial file
		dnsWriter.close();
		icmpWriter.close();
	}
	
}

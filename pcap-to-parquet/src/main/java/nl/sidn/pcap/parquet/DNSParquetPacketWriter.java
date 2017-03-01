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
package nl.sidn.pcap.parquet;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import nl.sidn.dnslib.message.Header;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.Question;
import nl.sidn.dnslib.message.records.edns0.ClientSubnetOption;
import nl.sidn.dnslib.message.records.edns0.DNSSECOption;
import nl.sidn.dnslib.message.records.edns0.EDNS0Option;
import nl.sidn.dnslib.message.records.edns0.NSidOption;
import nl.sidn.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidn.dnslib.message.records.edns0.PingOption;
import nl.sidn.dnslib.types.OpcodeType;
import nl.sidn.dnslib.types.RcodeType;
import nl.sidn.dnslib.types.ResourceRecordType;
import nl.sidn.dnslib.util.Domaininfo;
import nl.sidn.dnslib.util.IPUtil;
import nl.sidn.dnslib.util.NameUtil;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.ip.GoogleResolverCheck;
import nl.sidn.pcap.ip.OpenDNSResolverCheck;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.support.PacketCombination;
import nl.sidn.pcap.util.Settings;
import nl.sidn.stats.MetricManager;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.kitesdk.data.PartitionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DNSParquetPacketWriter extends AbstractParquetPacketWriter {

	private static final Logger LOGGER = LoggerFactory.getLogger(DNSParquetPacketWriter.class);
	
	private static final int RCODE_QUERY_WITHOUT_RESPONSE = -1;
	
	//metrics
	private Set<String> domainnames = new HashSet<>();
	private long responseBytes = 0;
	private long requestBytes = 0;
	private Map<Integer, Integer> qtypes = new HashMap<>();
	private Map<Integer, Integer> rcodes = new HashMap<>();
	private Map<Integer, Integer> opcodes = new HashMap<>();
	private int requestUDPFragmentedCount = 0;
	private int requestTCPFragmentedCount = 0;
	private int responseUDPFragmentedCount = 0;
	private int responseTCPFragmentedCount = 0;
	private int ipv4QueryCount = 0;
	private int ipv6QueryCount = 0;
	
	private MetricManager metricManager;
	private GoogleResolverCheck googleCheck = new GoogleResolverCheck();
	private OpenDNSResolverCheck openDNSCheck = new OpenDNSResolverCheck();
	
	public DNSParquetPacketWriter(String repoName, String schema) {
		super(repoName,schema);
		metricManager = MetricManager.getInstance();
	}

	private Question lookupQuestion(Message reqMessage, Message respMessage){
		if(reqMessage != null && reqMessage.getQuestions().size() > 0){
			return reqMessage.getQuestions().get(0);
		}else if(respMessage != null && respMessage.getQuestions().size() > 0){
			return respMessage.getQuestions().get(0);
		}
		//should never get here
		return null;
	}
	
	private Header lookupHeader(Message reqMessage, Message respMessage){
		if(reqMessage != null){
			return reqMessage.getHeader();
		}else if(respMessage != null){
			return respMessage.getHeader();
		}
		//should never get here
		return null;
	}
	
	private long lookupTime(Packet reqPacket, Packet respPacket){
		if(reqPacket != null){
			return reqPacket.getTs();
		}else if(respPacket != null){
			return respPacket.getTs();
		}
		//should never get here
		return -1;
	}
	
	/**
	 * create 1 parquet record which combines values from the query and the response
	 * @param packet
	 */
	@Override
	public void write(PacketCombination combo) {

		GenericRecordBuilder builder = newBuilder();
		
		packetCounter++;
		if(packetCounter % STATUS_COUNT == 0){
			showStatus();
		}
		Packet reqTransport = combo.getRequest();
		Message requestMessage = combo.getRequestMessage();
		Packet respTransport = combo.getResponse();
		Message respMessage = combo.getResponseMessage();
		
		Question q = lookupQuestion(requestMessage, respMessage);
		Header header = lookupHeader(requestMessage, respMessage);
		
		//get the time in milliseconds
		long time = lookupTime(reqTransport, respTransport);
		Timestamp ts = new Timestamp((time * 1000));

	    String normalizedQname =  q == null? "": filter(q.getqName());
	    normalizedQname = StringUtils.lowerCase(normalizedQname);	    
	    Domaininfo domaininfo = NameUtil.getDomain(normalizedQname, Settings.getTldSuffixes());
	    //check to see it a response was found, if not then save -1 value
	    //otherwise use the rcode returned by the server in the response.
	    //no response might be caused by rate limiting
	    int rcode = RCODE_QUERY_WITHOUT_RESPONSE;  //default no reply, use non standard rcode value -1

		//set the nameserver the queries are going to/coming from
		builder.set("svr", combo.getServer().getName());

		//if no anycast location is encoded in the name then the anycast location will be null
	    builder.set("server_location", combo.getServer().getLocation());

		//add meta data
		enrich(reqTransport, respTransport, builder);
		
	    //these are the values that are retrieved from the response
	    if(respTransport != null && respMessage != null){
	    	Header respHdr = respMessage.getHeader();
	    	rcode = respHdr.getRawRcode();
	   
	    	builder
		    	.set("aa",  respHdr.isAa())
			    .set("tc",  respHdr.isTc())
			    .set("ra",  respHdr.isRa())
			    .set("ad",  respHdr.isAd())
			    .set("ancount",  (int)respHdr.getAnCount())
			    .set("arcount",  (int)respHdr.getArCount())
			    .set("nscount",  (int)respHdr.getNsCount())
	    	  	.set("res_len", respTransport.getTotalLength())
	    	    .set("dns_res_len", respMessage.getBytes());
	    	
	    	//ip fragments in the response
	    	if(respTransport.isFragmented()){
	    		int frags = respTransport.getReassembledFragments();
	    		builder.set("resp_frag",frags );
	    		
	    		if((respTransport.getProtocol() == PcapReader.PROTOCOL_UDP) && frags > 1){
	    			responseUDPFragmentedCount++;
	    		}else if((respTransport.getProtocol() == PcapReader.PROTOCOL_TCP) && frags > 1){
	    			responseTCPFragmentedCount++;
	    		}
	    	}
	    	
	    	//EDNS0 for response
			writeResponseOptions(respMessage, builder);
	    	
	    	//update metric
    		responseBytes = responseBytes + respTransport.getUdpLength();
	    	if(!combo.isExpired()){
	    		//do not send expired queries, this will cause duplicate timestamps with low values
	    		//this looks like dips in the grafana graph
	    		metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_RESPONSE_COUNT, 1, time);
	    	}
	    }
	    
	    //values from request now, if no request found then use parts of the response.
	    builder
	    	.set("id", header.getId())
			.set("rcode", rcode)
		    .set("opcode",  header.getRawOpcode())
			.set("unixtime", reqTransport != null? reqTransport.getTs(): respTransport.getTs())
			.set("time",  ts.getTime())
			.set("time_micro", reqTransport != null? reqTransport.getTsmicros(): respTransport.getTsmicros())
			.set("qname",normalizedQname )
		    .set("domainname", domaininfo.name)
		    .set("labels",  domaininfo.labels)
		    .set("src",  reqTransport != null? reqTransport.getSrc(): respTransport.getDst())
		    .set("len",  reqTransport != null? reqTransport.getTotalLength(): null)
		    .set("ttl",  reqTransport != null? reqTransport.getTtl(): null)
		    .set("ipv",  reqTransport != null? (int)reqTransport.getIpVersion(): (int)respTransport.getIpVersion())
		    .set("prot", reqTransport != null? (int)reqTransport.getProtocol(): (int)respTransport.getProtocol())
		    .set("srcp",  reqTransport != null? reqTransport.getSrcPort(): null)
		    .set("dst",  reqTransport != null? reqTransport.getDst(): respTransport.getSrc())
		    .set("dstp",  reqTransport != null? reqTransport.getDstPort(): respTransport.getSrcPort())
		    .set("udp_sum",  reqTransport != null? reqTransport.getUdpsum(): null)
		    .set("dns_len",  requestMessage != null? requestMessage.getBytes(): null)
		    .set("rd",  header.isRd())
		    .set("z",  header.isZ())
		    .set("cd",  header.isCd())
		    .set("qdcount",  (int)header.getQdCount());

	    if(reqTransport != null){
	    	
	  	 //ip fragments in the request
			if(reqTransport.isFragmented()){
				int req_frags = reqTransport.getReassembledFragments();
				builder.set("frag", req_frags);
				
				if((reqTransport.getProtocol() == PcapReader.PROTOCOL_UDP) && req_frags > 1){
					requestUDPFragmentedCount++;
				}else if((reqTransport.getProtocol() == PcapReader.PROTOCOL_TCP) && req_frags > 1){
					requestTCPFragmentedCount++;
				}
			}
			
			//update metrics
	    	requestBytes = requestBytes + reqTransport.getUdpLength();
	    	if(!combo.isExpired()){
	    		//do not send expired queries, this will cause duplicate timestamps with low values
	    		//this looks like dips in the grafana graph
	    		metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_QUERY_COUNT, 1, time);
	    	}
		}
	    
	    if(rcode == RCODE_QUERY_WITHOUT_RESPONSE){
	    	//no response found for query, update stats
	    	metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_NO_RESPONSE_COUNT, 1, time);
	    }
	    
	    //question
	    writeQuestion(q, builder);
		
		//EDNS0 for request
		writeRequestOptions(requestMessage, builder);
		 
		//calculate the processing time
		writeProctime(reqTransport, respTransport, builder);

		//create the actual record and write to parquet file
		GenericRecord record = builder.build();
		writer.write(record);
		
		//create metrics
		domainnames.add(domaininfo.name);
		updateMetricMap(rcodes, rcode);
		updateMetricMap(opcodes, header.getRawOpcode());
		
		//ip version stats
		updateIpVersionMetrics(reqTransport, respTransport);
		
		//if packet was expired and dropped from cache then increase stats for this
		if(combo.isExpired()){
			metricManager.sendAggregated(MetricManager.METRIC_IMPORT_CACHE_EXPPIRED_DNS_QUERY_COUNT, 1, time, false);
		}
	}
	
	private void enrich(Packet reqPacket,Packet respPacket, GenericRecordBuilder builder){
		String country = null;
	    String asn = null;
	    boolean isGoogle = false;
	    boolean isOpenDNS = false;
		if(reqPacket != null){
			//request packet, check the source address
			country = getCountry(reqPacket.getSrc());
		    asn = getAsn(reqPacket.getSrc(), reqPacket.getIpVersion() == 4);
		    isGoogle = googleCheck.isMatch(reqPacket.getSrc());
		    if(!isGoogle){
		    	isOpenDNS = openDNSCheck.isMatch(reqPacket.getSrc());
		    }
		}else{
			//response packet, check the destination address
			country = getCountry(respPacket.getDst());
		    asn = getAsn(respPacket.getDst(), respPacket.getIpVersion() == 4);
		    isGoogle = googleCheck.isMatch(respPacket.getDst());
		    if(!isGoogle){
		    	isOpenDNS = openDNSCheck.isMatch(respPacket.getDst());
		    }
		}
		
		builder.set("country",  country)
        .set("asn", asn)
		//check of the resolver is a google backend resolver
	    .set("is_google", isGoogle)
		//check of the resolver is a opendns backend resolver
	    .set("is_opendns", isOpenDNS);
	}

	private void updateIpVersionMetrics(Packet req, Packet resp){
		if (req != null){
			if(req.getIpVersion() == 4){
				ipv4QueryCount++;
			}else{
				ipv6QueryCount++;
			}
		}else{
			if(resp.getIpVersion() == 4){
				ipv4QueryCount++;
			}else{
				ipv6QueryCount++;
			}
		}
	}

	private void writeQuestion(Question q, GenericRecordBuilder builder) {
		if(q != null){
			//unassigned, private or unknown, get raw value
			builder.set("qtype",  q.getqTypeValue());
			//unassigned, private or unknown, get raw value
			builder.set("qclass",  q.getqClassValue());
			//qtype metrics
			updateMetricMap(qtypes, q.getqTypeValue());
		}
	}


	//calc the number of seconds between receivinfg the response and sending it back to the resolver
	private void writeProctime(Packet reqTransport, Packet respTransport, GenericRecordBuilder builder) {
		if(reqTransport != null && respTransport != null){
			Timestamp reqTs = new Timestamp((reqTransport.getTs() * 1000000));
			Timestamp respTs = new Timestamp((respTransport.getTs() * 1000000));
			
			//from second to microseconds
			long millis1 = respTs.getTime() - reqTs.getTime();
			long millis2 = (respTransport.getTsmicros() - reqTransport.getTsmicros());
			builder.set("proc_time", millis1 + millis2);
		}
	}

	/**
	 * Write EDNS0 option (if any are present) to file. 
	 * @param message
	 * @param builder
	 */
	private void writeResponseOptions(Message message, GenericRecordBuilder builder) {
		if(message == null){
			return;
		}
		
		OPTResourceRecord opt = message.getPseudo();
		if(opt != null){
			 for (EDNS0Option option : opt.getOptions()) {
		        if(option instanceof NSidOption){
		        	String id = ((NSidOption)option).getId();
		        	builder.set("edns_nsid", id!= null? id: "");
		        	
		        	//this is the only server edns data we support, stop processing other options
		        	break;
		        }
			 }
		}   
		
	}

	/**
	 * Write EDNS0 option (if any are present) to file. 
	 * @param message
	 * @param builder
	 */
	private void writeRequestOptions(Message message, GenericRecordBuilder builder) {
		if(message == null){
			return;
		}
		
		OPTResourceRecord opt = message.getPseudo();
		 if(opt != null){
	        builder
	        .set("edns_udp", (int)opt.getUdpPlayloadSize())
	        .set("edns_version", (int)opt.getVersion())
	        .set("edns_do", opt.getDnssecDo());
	        
	        String other = null;
	        for (EDNS0Option option : opt.getOptions()) {
	        	if(option instanceof PingOption){
	        		builder.set("edns_ping", true);
	        	}else if(option instanceof DNSSECOption){
	        		if(option.getCode() == DNSSECOption.OPTION_CODE_DAU){
	        			builder.set("edns_dnssec_dau", ((DNSSECOption)option).export());
	        		}else if(option.getCode() == DNSSECOption.OPTION_CODE_DHU){
	        			builder.set("edns_dnssec_dhu", ((DNSSECOption)option).export());
	        		}else{ //N3U
	        			builder.set("edns_dnssec_n3u", ((DNSSECOption)option).export());
	        		}
	        	}else if(option instanceof ClientSubnetOption){
	        		ClientSubnetOption scOption = (ClientSubnetOption)option;
	        		//get client country and asn
	        		String clientCountry = null;
	        		String clientASN = null;
	        		if(scOption.getAddress() != null){
		        		if(scOption.isIPv4()){
		        			try{
			        			byte[] addrBytes= IPUtil.ipv4tobytes(scOption.getAddress());
			        			clientCountry = geoLookup.lookupCountry(addrBytes);
			        			clientASN = geoLookup.lookupASN(addrBytes, true);
		        			}catch(Exception e){
		        				LOGGER.error("Could not convert IPv4 addr to bytes, invalid address? :" + scOption.getAddress());
		        			}
		        		}else{
		        			try{
			        			byte[] addrBytes= IPUtil.ipv6tobytes(scOption.getAddress());
			        			clientCountry = geoLookup.lookupCountry(addrBytes);
			        			clientASN = geoLookup.lookupASN(addrBytes, false);
		        			}catch(Exception e){
		        				LOGGER.error("Could not convert IPv6 addr to bytes, invalid address? :" + scOption.getAddress());
		        			}
		        		}
	        		}	
	        		builder.set("edns_client_subnet",  scOption.export())
 		    			.set("edns_client_subnet_asn", clientASN)
 		    			.set("edns_client_subnet_country", clientCountry);
	        		
	        	}else{
	        		//other
	        		if(StringUtils.length(other) > 0){
	        			other = other + "," + option.getCode();
	        		}else{
	        			other = String.valueOf(option.getCode());
	        		}
	        	}
			}
	        
	        if(other != null){
	        	builder.set("edns_other", other);
	        }
		 }
	}


	@Override
	protected PartitionStrategy createPartitionStrategy(){
		return new PartitionStrategy.Builder().year("time").month("time").day("time").identity("svr","server").build();
	}
	
	@Override
	public void writeMetrics(){
		
		for(Integer key: rcodes.keySet()){
			Integer value = rcodes.get(key);
			if(key.intValue() == -1){
				//pseudo rcode -1 means no reponse, not an official rcode
				metricManager.send(MetricManager.METRIC_IMPORT_DNS_RCODE +  ".NO_RESPONSE.count", value.intValue() );
			}else{
				RcodeType type = RcodeType.fromValue(key.intValue());
				metricManager.send(MetricManager.METRIC_IMPORT_DNS_RCODE + StringUtils.upperCase("."+type.name()) + ".count", value.intValue() );
			}
		}
		
		metricManager.send(MetricManager.METRIC_IMPORT_IP_COUNT,geo_ip_cache.size() );
		metricManager.send(MetricManager.METRIC_IMPORT_COUNTRY_COUNT,countries.size() );
		metricManager.send(MetricManager.METRIC_IMPORT_ASN_COUNT,asn_cache.size() );
		
		metricManager.send(MetricManager.METRIC_IMPORT_DNS_DOMAINNAME_COUNT, domainnames.size() );
		if(responseBytes > 0){
			metricManager.send(MetricManager.METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE, (int)(responseBytes/1024));
		}else{
			metricManager.send(MetricManager.METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE, 0);
		}
		if(requestBytes > 0){
			metricManager.send(MetricManager.METRIC_IMPORT_DNS_QUERY_BYTES_SIZE, (int)(requestBytes/1024));
		}else{
			metricManager.send(MetricManager.METRIC_IMPORT_DNS_QUERY_BYTES_SIZE, 0);
		}
		
		for(Integer key: qtypes.keySet()){
			Integer value = qtypes.get(key);
			ResourceRecordType type = ResourceRecordType.fromValue(key.intValue());
			metricManager.send(MetricManager.METRIC_IMPORT_DNS_QTYPE + StringUtils.upperCase("."+type.name()) + ".count", value.intValue() );
		}
		
		for(Integer key: opcodes.keySet()){
			Integer value = opcodes.get(key);
			OpcodeType type = OpcodeType.fromValue(key.intValue());
			metricManager.send(MetricManager.METRIC_IMPORT_DNS_OPCODE + StringUtils.upperCase("."+type.name()) + ".count", value.intValue() );
		}
		
		metricManager.send(MetricManager.METRIC_IMPORT_UDP_REQUEST_FRAGMENTED_COUNT, requestUDPFragmentedCount);
		metricManager.send(MetricManager.METRIC_IMPORT_TCP_REQUEST_FRAGMENTED_COUNT, requestTCPFragmentedCount);
		metricManager.send(MetricManager.METRIC_IMPORT_UDP_RESPONSE_FRAGMENTED_COUNT, responseUDPFragmentedCount);
		metricManager.send(MetricManager.METRIC_IMPORT_TCP_RESPONSE_FRAGMENTED_COUNT, responseTCPFragmentedCount);
		
		metricManager.send(MetricManager.METRIC_IMPORT_IP_VERSION_4_COUNT, ipv4QueryCount);
		metricManager.send(MetricManager.METRIC_IMPORT_IP_VERSION_6_COUNT, ipv6QueryCount);
		
	}

}


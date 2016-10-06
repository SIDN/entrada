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
import java.util.Map;

import nl.sidn.dnslib.message.Header;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.Question;
import nl.sidn.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidn.dnslib.util.Domaininfo;
import nl.sidn.dnslib.util.NameUtil;
import nl.sidn.pcap.packet.DNSPacket;
import nl.sidn.pcap.packet.ICMPPacket;
import nl.sidn.pcap.packet.Packet;
import nl.sidn.pcap.support.PacketCombination;
import nl.sidn.pcap.util.Settings;
import nl.sidn.stats.MetricManager;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.kitesdk.data.PartitionStrategy;

public class ICMPParquetPacketWriter extends AbstractParquetPacketWriter {

	//stats counters
	private int v4;
	private int v6;
	private int typeError;
	private int typeInfo;
	private Map<Integer, Integer> types_v4 = new HashMap<>();
	private Map<Integer, Integer> types_v6 = new HashMap<>();

	public ICMPParquetPacketWriter(String repoName, String schema) {
		super(repoName,schema);
	}

	@Override
	public void write(PacketCombination packetCombo) {
		
		GenericRecordBuilder builder = newBuilder();

		ICMPPacket icmpPacket = (ICMPPacket)packetCombo.getRequest();
		
		Packet originalPacket = null;
		Message dnsResponseMessage = null;
		ICMPPacket originalICMPPacket = null;
		
		packetCounter++;
		if(packetCounter % STATUS_COUNT == 0){
			showStatus();
		}
		
		//get message .nl auth send to client and which is returned in the icmp payload
		if(icmpPacket.getOriginalIPPacket() != null && icmpPacket.getOriginalIPPacket() != Packet.NULL ){
			originalPacket = icmpPacket.getOriginalIPPacket();
		}
		
		if(originalPacket instanceof DNSPacket ){
			dnsResponseMessage = ((DNSPacket)originalPacket).getMessage();
		}else if(originalPacket instanceof ICMPPacket ){
			originalICMPPacket = (ICMPPacket)originalPacket;
		}
		
		//icmp packet ip+headers
		Timestamp packetTime = new Timestamp((icmpPacket.getTs() * 1000));			
		String country = getCountry(icmpPacket.getSrc());
	    String asn = getAsn(icmpPacket.getSrc(), icmpPacket.getIpVersion() == 4);

	    //icmp payload	
		Question q = null;
		Header dnsResponseHdr = null;
		Domaininfo domaininfo = null;
		String normalizedQname = null;
		
		if(dnsResponseMessage != null){
		    //malformed (bad_format) message can have missing question
			if(dnsResponseMessage.getQuestions().size() > 0){
				q = dnsResponseMessage.getQuestions().get(0);
			}
			dnsResponseHdr = dnsResponseMessage.getHeader();
		    normalizedQname =  q == null? "": filter(q.getqName());
		    normalizedQname = StringUtils.lowerCase(normalizedQname);    
		    domaininfo = NameUtil.getDomain(normalizedQname, Settings.getTldSuffixes());
		}

	    //values from query now.
	    builder.set("svr", packetCombo.getServer().getName())
	        .set("unixtime", icmpPacket.getTs())
	        .set("time_micro", icmpPacket.getTsmicros())
	    	.set("time",  packetTime.getTime())
	    	.set("icmp_type",  icmpPacket.getType())
	    	.set("icmp_code",  icmpPacket.getCode())
	    	.set("icmp_echo_client_type", icmpPacket.getClientType())
		    .set("ip_ttl",  icmpPacket.getTtl())
		    .set("ip_v",  (int)icmpPacket.getIpVersion())
		    .set("ip_src",  icmpPacket.getSrc())
		    .set("ip_dst",  icmpPacket.getDst())
		    .set("ip_country",  country)
	        .set("ip_asn", asn)
		    .set("l4_prot",  (int)icmpPacket.getProtocol())
		    .set("l4_srcp",  icmpPacket.getSrcPort())
		    .set("l4_dstp",  icmpPacket.getDstPort())
		    .set("ip_len",  icmpPacket.getTotalLength()); //size of ip packet incl headers

	    //if no anycast location is encoded in the name then the anycast server name and location will be null
    	//only store this column in case of anycast, to save storage space.
    	//the server name can be determined with the "svr" column
	    builder.set("server_location", packetCombo.getServer().getLocation());
	  
	    //orig packet from payload
		 
	    if(originalPacket != null && originalPacket != Packet.NULL){
	    	
	    	builder.set("orig_ip_ttl",  originalPacket.getTtl())
		    .set("orig_ip_v",  (int)originalPacket.getIpVersion())
		    .set("orig_ip_src",  originalPacket.getSrc())
		    .set("orig_ip_dst",  originalPacket.getDst())
		    .set("orig_l4_prot",  (int)originalPacket.getProtocol())
		    .set("orig_l4_srcp",  originalPacket.getSrcPort())
		    .set("orig_l4_dstp",  originalPacket.getDstPort())
	    	.set("orig_udp_sum",  originalPacket.getUdpsum())
		    .set("orig_ip_len",  originalPacket.getTotalLength()); //size of ip packet incl headers
	    
		    if(originalICMPPacket != null){
		    	builder.set("orig_icmp_type",  originalICMPPacket.getType())
		    	.set("orig_icmp_code",  originalICMPPacket.getCode())
		    	.set("orig_icmp_echo_client_type", originalICMPPacket.getClientType());
		    }
	    
		    if(dnsResponseMessage != null){
			    //orig dns response from icmp packet
		    	builder
		    	.set("orig_dns_len", originalPacket.getPayloadLength()) //get the size from the reassembled udp header of the original udp response
		    	.set("orig_dns_id", dnsResponseHdr.getId())
		    	.set("orig_dns_qname",normalizedQname )
			    .set("orig_dns_domainname", domaininfo.name)  
			    .set("orig_dns_aa",  dnsResponseHdr.isAa())
			    .set("orig_dns_tc",  dnsResponseHdr.isTc())
			    .set("orig_dns_rd",  dnsResponseHdr.isRd())
			    .set("orig_dns_ra",  dnsResponseHdr.isRa())
			    .set("orig_dns_z",  dnsResponseHdr.isZ())
			    .set("orig_dns_ad",  dnsResponseHdr.isAd())
			    .set("orig_dns_cd",  dnsResponseHdr.isCd())
			    .set("orig_dns_ancount",  (int)dnsResponseHdr.getAnCount())
			    .set("orig_dns_arcount",  (int)dnsResponseHdr.getArCount())
			    .set("orig_dns_nscount",  (int)dnsResponseHdr.getNsCount())
			    .set("orig_dns_qdcount",  (int)dnsResponseHdr.getQdCount())  
				.set("orig_dns_rcode", dnsResponseHdr.getRawRcode())
			    .set("orig_dns_opcode",  dnsResponseHdr.getRawOpcode())
		        //set edns0 defaults
		        .set("orig_dns_edns_udp", null)
			    .set("orig_dns_edns_version", null)
			    .set("orig_dns_edns_do",null)
			    .set("orig_dns_labels",  domaininfo.labels);

				 if(q != null){
					 //unassinged, private or unknown, get raw value
					 builder.set("orig_dns_qtype",  q.getqTypeValue());
					 //unassinged, private or unknown, get raw value
					 builder.set("orig_dns_qclass",  q.getqClassValue());
				 }
				 
				 OPTResourceRecord opt = dnsResponseMessage.getPseudo();
				 if(opt != null){
			        builder
			        .set("orig_dns_edns_udp", (int)opt.getUdpPlayloadSize())
			        .set("orig_dns_edns_version", (int)opt.getVersion())
			        .set("orig_dns_edns_do", opt.getDnssecDo());
				 }
		    }
	    }

		GenericRecord record = builder.build();
		writer.write(record);

		//write stats
		if(icmpPacket.isIPv4()){
			v4++;
			updateMetricMap(types_v4, new Integer(icmpPacket.getType()));
		}else{
			v6++;
			updateMetricMap(types_v6, new Integer(icmpPacket.getType()));
		}
		
		if(icmpPacket.isError()){
			typeError++;
		}else{
			typeInfo++;
		}
	}
	

	@Override
	protected PartitionStrategy createPartitionStrategy(){
		return new PartitionStrategy.Builder().year("time").month("time").day("time").build();
	}
	
	@Override
	public void writeMetrics(){
		MetricManager mm = MetricManager.getInstance();
		mm.send(MetricManager.METRIC_ICMP_V4,v4);
		mm.send(MetricManager.METRIC_ICMP_V4,v6);
		writeMetrics(mm, types_v4, MetricManager.METRIC_ICMP_PREFIX_TYPE_V4);
		writeMetrics(mm, types_v6, MetricManager.METRIC_ICMP_PREFIX_TYPE_V6);
		mm.send(MetricManager.METRIC_ICMP_ERROR,typeError);
		mm.send(MetricManager.METRIC_ICMP_INFO,typeInfo);
	}
	
	protected void writeMetrics(MetricManager mm,Map<Integer, Integer> map, String prefix){
		for(Integer key: map.keySet()){
			Integer value = map.get(key);
			mm.send(prefix + "." + value + ".count", value.intValue() );
		}
	}


}


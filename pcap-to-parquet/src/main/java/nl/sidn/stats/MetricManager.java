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
package nl.sidn.stats;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import nl.sidn.pcap.util.Settings;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricManager {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MetricManager.class);
	
	//dns stats
	public static String METRIC_IMPORT_DNS_QUERY_COUNT = ".dns.request.count";
	public static String METRIC_IMPORT_DNS_RESPONSE_COUNT = ".dns.response.count";	
	public static String METRIC_IMPORT_DNS_NO_REQUEST_COUNT = ".dns.response.norequest.count";
	public static String METRIC_IMPORT_DNS_QTYPE = ".dns.request.qtype";
	public static String METRIC_IMPORT_DNS_RCODE = ".dns.request.rcode";
	public static String METRIC_IMPORT_DNS_OPCODE = ".dns.request.opcode";
	public static String METRIC_IMPORT_DNS_NO_RESPONSE_COUNT = ".dns.noreresponse.count";
	
	//layer 4 stats
	public static String METRIC_IMPORT_DNS_TCPSTREAM_COUNT = ".dns.tcp.session.count";
	public static String METRIC_IMPORT_TCP_COUNT = ".tcp.packet.count";
	public static String METRIC_IMPORT_UDP_COUNT = ".udp.packet.count";
	
	public static String METRIC_IMPORT_UDP_REQUEST_FRAGMENTED_COUNT = ".udp.request.fragmented.count";
	public static String METRIC_IMPORT_UDP_RESPONSE_FRAGMENTED_COUNT = ".udp.response.fragmented.count";
	public static String METRIC_IMPORT_TCP_REQUEST_FRAGMENTED_COUNT = ".tcp.request.fragmented.count";	
	public static String METRIC_IMPORT_TCP_RESPONSE_FRAGMENTED_COUNT = ".tcp.response.fragmented.count";
	public static String METRIC_IMPORT_IP_VERSION_4_COUNT = ".ip.version.4.count";
	public static String METRIC_IMPORT_IP_VERSION_6_COUNT = ".ip.version.6.count";
		
	public static String METRIC_IMPORT_IP_COUNT = ".ip.count";
	public static String METRIC_IMPORT_COUNTRY_COUNT = ".country.count";
	public static String METRIC_IMPORT_ASN_COUNT = ".asn.count";
	public static String METRIC_IMPORT_DNS_DOMAINNAME_COUNT = ".dns.domainname.count";
	public static String METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE = ".dns.response.bytes.size";
	public static String METRIC_IMPORT_DNS_QUERY_BYTES_SIZE = ".dns.request.bytes.size";

	//decoder app stats
	public static String METRIC_IMPORT_DNS_COUNT = ".dns.message.count";
	public static String METRIC_IMPORT_FILES_COUNT = ".files.count";
	public static String METRIC_IMPORT_RUN_TIME = ".time.duration";
	public static String METRIC_IMPORT_RUN_ERROR_COUNT = ".run.error.count";
	
	public static String METRIC_IMPORT_TCP_PREFIX_ERROR_COUNT = ".tcp.prefix.error.count";
	public static String METRIC_IMPORT_DNS_DECODE_ERROR_COUNT = ".dns.decode.error.count";
	
	public static String METRIC_IMPORT_STATE_PERSIST_UDP_FLOW_COUNT = ".state.persist.udp.flow.count";
	public static String METRIC_IMPORT_STATE_PERSIST_TCP_FLOW_COUNT = ".state.persist.tcp.flow.count";
	public static String METRIC_IMPORT_STATE_PERSIST_DNS_COUNT = ".state.persist.dns.count";
	
	//icmp
	public static String METRIC_ICMP_COUNT = ".icmp.packet.count";
	public static String METRIC_ICMP_V4 = ".icmp.v4";
	public static String METRIC_ICMP_V6 = ".icmp.v6";
	public static String METRIC_ICMP_PREFIX_TYPE_V4 = ".icmp.v4.prefix.type";
	public static String METRIC_ICMP_PREFIX_TYPE_V6 = ".icmp.v6.prefix.type";
	public static String METRIC_ICMP_ERROR = ".icmp.error";
	public static String METRIC_ICMP_INFO = ".icmp.info";
	
	//cache stats
	public static String METRIC_IMPORT_CACHE_EXPPIRED_DNS_QUERY_COUNT = ".cache.expired.dns.request.count";
	
	private static MetricManager _metricManager = null;
	private PersistenceManager metricPersistenceManager = null;
	
	private Map<String, Metric> metricCache = new HashMap<String, Metric>();
	private List<Metric> realtimeMetrics = new ArrayList<>();
	
	private String graphitePrefix = StringUtils.trimToEmpty(Settings.getInstance().getSetting("graphite.prefix"));
	private int retention = Settings.getInstance().getIntSetting("graphite.retention");
	private int threshhold = Settings.getInstance().getIntSetting("graphite.threshhold");
	
	public static MetricManager getInstance(){
		if(_metricManager == null){
			_metricManager = new MetricManager();
		}
		return _metricManager;
	}

	private MetricManager(){
		metricPersistenceManager = new PersistenceManager(this);
	}
	
	/**
	 * send overall metrics, use current system time
	 * @param metric
	 * @param value
	 */
	public void send(String metric, int value){
		String metricName = createMetricName(metric);
		TimeZone timeZone = TimeZone.getTimeZone("UTC");
		Calendar calendar = Calendar.getInstance(timeZone);
		realtimeMetrics.add(new Metric(metricName, value,calendar.getTimeInMillis()/1000));
	}

	private String createMetricName(String metric){
		//replace dot in the server name with underscore otherwise graphite will assume nesting
		String cleanServer =  StringUtils.trimToEmpty(StringUtils.replace(Settings.getInstance().getServer().getFullname(),".","_"));
		return graphitePrefix + "." + cleanServer + metric;
	}

	
	public void sendAggregated(String metric, int value, long timestamp, boolean useThreshHold){
		long metricTime = roundTimestamp(timestamp);
		String metricName = createMetricName(metric);
		String lookup = metricName + "." + metricTime;
		Metric m = metricCache.get(lookup);
		if(m != null){
			m.update(value);
		}else{
			metricCache.put(lookup, new Metric(metricName, value,metricTime, useThreshHold));
		}
	}
	
	/**
	 * send aggregated counts (per server) aggregate by 10s bucket
	 * @param metric
	 * @param value
	 * @param timestamp
	 * @param server
	 */
	public void sendAggregated(String metric, int value, long timestamp){
		sendAggregated(metric, value, timestamp, true);
	}
	
	public void flush(){
		if(LOGGER.isDebugEnabled()){
			LOGGER.debug("Write metrics to queue");
			for(String key : metricCache.keySet()){
				LOGGER.debug("Metric key: " + key);
				LOGGER.debug("Metric value: " + metricCache.get(key));
			}
		}
		
		GraphiteAdapter graphiteAdapter = new GraphiteAdapter();
		graphiteAdapter.connect();
		
		StringBuffer buffer = new StringBuffer();
		if(graphiteAdapter.isConnected()){
			for(String key : metricCache.keySet()){
				Metric m = metricCache.get(key);
				
				/**
				 * Use a threshhold to determine if the value should be sent to graphite
				 * low values may indicate trailing queries in later pcap files.
				 * duplicate timestamps get overwritten by graphite and only the last timestamp value
				 * is used by graphite.
				 */
				if(m.isUseThreshHold() && m.getValue() < threshhold){ 
					LOGGER.debug("Metric " + m.getName() + " is below threshold min of " + threshhold
							+ " with actual value of " + m.getValue());
				}else{
					//add metric to graphite plaintext protocol string
					//@see: http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol
					buffer.append(m.toString() + "\n");
				}
			}
			
			for(Metric m : realtimeMetrics){
				if(m != null){
					buffer.append(m.toString() + "\n");
				}
			}
			LOGGER.info("Send metrics: " + buffer.toString());
			graphiteAdapter.send(buffer.toString());
			graphiteAdapter.close();
		}
	}
	
	
	/**
	 * Round the timestamp to the nearest retention time
	 * @param intime
	 * @return
	 */
	private long roundTimestamp(long intime){
		//get retention from config
		long offset = (intime % retention);
	    return intime - offset;
	}
	
	
	protected Map<String, Metric> getMetricCache() {
		return metricCache;
	}

	protected void setMetricCache(Map<String, Metric> metricCache) {
		this.metricCache = metricCache;
	}

	public PersistenceManager getMetricPersistenceManager() {
		return metricPersistenceManager;
	}

	public void setMetricPersistenceManager(PersistenceManager metricPersistenceManager) {
		this.metricPersistenceManager = metricPersistenceManager;
	}


}

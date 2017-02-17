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
package nl.sidn.pcap.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import nl.sidn.dnslib.util.DomainParent;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for reading the entrada config file and making the
 * key-value pairs available.
 *
 */
public class Settings {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Settings.class);

	public static String INPUT_LOCATION = "input.location";
	public static String OUTPUT_LOCATION = "output.location";
	public static String STATE_LOCATION = "state.location";
	public static String OUTPUT_MAX_PACKETS = "output.max.packets";
	public static String CACHE_TIMEOUT = "cache.timeout";
	public static String CACHE_TIMEOUT_TCP_FLOW = "cache.timeout.tcp.flows";
	public static String CACHE_TIMEOUT_FRAG_IP = "cache.timeout.ip.fragmented";
	
	public static String METRICS_EXCHANGE = "metrics.exchange";
	public static String METRICS_QUEUE = "metrics.queue";
	public static String METRICS_USERNAME = "metrics.username";
	public static String METRICS_PASSWORD = "metrics.password";
	public static String METRICS_VIRTUALHOST = "metrics.virtualhost";
	public static String METRICS_HOST = "metrics.host";
	public static String METRICS_TIMEOUT = "metrics.timeout";
	
	public static String RESOLVER_LIST_GOOGLE = "resolver.list.google";
	public static String RESOLVER_LIST_OPENDNS = "resolver.list.opendns";
	
	public static String TLD_SUFFIX = "tld.suffix";
	public static String BUFFER_PCAP_READER = "buffer.pcap.reader";
	
	
	private static Properties props = null;
	private static Settings _instance = null;
	
	private static String path = null;
	private ServerInfo serverInfo = null;
	
	private static List<DomainParent> tldSuffixes = new ArrayList<>();
	
	private Settings(String path){
		init(path);
		debug();
	}
	
	public static Settings getInstance(){
		if(_instance == null){
			_instance = new Settings(path);
		}
		return _instance;
	}
	
	public static void init(String path) {
		 
		props = new Properties();
		InputStream input = null;
	 
		try {
	 
			input = new FileInputStream(path);
			props.load(input);
	 
		} catch (IOException e) {
			throw new RuntimeException("Could not load settings", e);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					//ignore exception while closing
					LOGGER.error("Could not close settings",e );
				}
			}
		}
		//do other init work
		createTldSuffixes();
	  }
	
	public String getSetting(String key){
		return props.getProperty(key);
	}

	public void setSetting(String key, String value){
		props.setProperty(key, value);
	}
	
	public int getIntSetting(String key){
		try {
			return Integer.parseInt(props.getProperty(key));
		} catch (NumberFormatException e) {
			throw new RuntimeException("Value " + props.getProperty(key) + " for " + key + " is not a valid number", e);
		}
	}
	
	public int getIntSetting(String key, int defaultValue){
		try {
			return Integer.parseInt(props.getProperty(key));
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public static void setPath(String settingFilePath){
		path = settingFilePath;
		_instance = null;
	}

	public ServerInfo getServer() {
		return serverInfo;
	}

	/**
	 * Load the server and optional anycast server location information.
	 * Using format <server>_<location>
	 */
	public void forServer(String name) {
		this.serverInfo = new ServerInfo();
		//set the pcap input directory name.
		serverInfo.setFullname(name);
		if (name.contains("_")) {
			String[] parts = StringUtils.split(name,"_"); 
			if(parts.length == 2){
				serverInfo.setName(parts[0]);
				serverInfo.setLocation(parts[1]);
				return;
			}
		}
		//no anycast location encoded in name
		serverInfo.setName(name);
	}


	public class ServerInfo{
		private String fullname;
		private String name;
		private String location;
		
		public String getFullname() {
			return fullname;
		}
		public void setFullname(String fullname) {
			this.fullname = fullname;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getLocation() {
			return location;
		}
		public void setLocation(String location) {
			this.location = location;
		}
		
		public boolean hasAnycastLocation(){
			return StringUtils.isNotBlank(location);
		}
		
	}
	
	public static void createTldSuffixes() {
		createTldSuffixes(props.getProperty(Settings.TLD_SUFFIX));
	}
	
	public static void createTldSuffixes(String value) {
		if(StringUtils.isEmpty(value)){
			//no value found, do nothing
			return;
		}
		
		String[] tlds = StringUtils.split(value, ",");
		//create list of DomainParents
		for (int i = 0; i < tlds.length; i++) {
			String parent = tlds[i];
			if(parent == null){
				//skip nulls
				continue;
			}
			//start and end with a dot.
			if(!StringUtils.startsWith(parent, ".")){
				parent = "." + parent;
			}
			
			int labelCount = StringUtils.split(parent,'.').length;
			if(StringUtils.endsWith(parent, ".")){
				//remove last dot (will become the used tld suffix
				tldSuffixes.add(new DomainParent(parent,StringUtils.removeEnd(parent, "."), labelCount));
			}else{
				tldSuffixes.add(new DomainParent(parent + ".",parent,labelCount));
			}
		}
		
	}

	public static List<DomainParent> getTldSuffixes() {
		return tldSuffixes;
	}
	
	private void debug(){
		LOGGER.info("************** entrada-setting.properties ******************");
		for( String pn : props.stringPropertyNames()){
			LOGGER.info(pn + ": " + props.getProperty(pn));
		}
		LOGGER.info("*************************************************************");
	}
}

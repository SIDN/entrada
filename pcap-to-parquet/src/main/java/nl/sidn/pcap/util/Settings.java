package nl.sidn.pcap.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
	
//	public static String MAXMIND_PATH = "maxmind.path";
//	public static String MAXMIND_GEOIP_PATH = "maxmind.geo.ip.path";
//	public static String MAXMIND_ASN_V4_PATH = "maxmind.geo.asn.v4.path";
//	public static String MAXMIND_ASN_V6_PATH = "maxmind.geo.asn.v6.path";
	
	public static String METRICS_EXCHANGE = "metrics.exchange";
	public static String METRICS_QUEUE = "metrics.queue";
	public static String METRICS_USERNAME = "metrics.username";
	public static String METRICS_PASSWORD = "metrics.password";
	public static String METRICS_VIRTUALHOST = "metrics.virtualhost";
	public static String METRICS_HOST = "metrics.host";
	public static String METRICS_TIMEOUT = "metrics.timeout";
	
	public static String RESOLVER_LIST_GOOGLE = "resolver.list.google";
	public static String RESOLVER_LIST_OPENDNS = "resolver.list.opendns";
	
	private static Properties props = null;
	private static Settings _instance = null;
	
	private static String path = null;
	private String name = null;
	
	private Settings(String path){
		init(path);
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
	
	public static void setPath(String settingFilePath){
		path = settingFilePath;
		_instance = null;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}


	
	
}

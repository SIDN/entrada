package nl.sidn.stats;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import nl.sidn.pcap.util.Settings;

public class PersistenceManager {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceManager.class);
	private MetricManager metricManager;
	
	public PersistenceManager(MetricManager metricManager){
		this.metricManager = metricManager;
	}

		
	public void persist(Kryo kryo, Output output){
		//only keep newly created, updated and non-expired metrics
		Map<String, Metric> metricCache = metricManager.getMetricCache();
		//get the max ttl for cache items, add 10 minutes just to be safe 
		//in cases where transporting the file to entrada takes longer 
		int cacheTimeoutInSecs = (Settings.getInstance().getIntSetting("cache.timeout") + 10)* 60;
		long now = System.currentTimeMillis();
		
		Map<String, Metric> newMap = new HashMap<>();
		int expired = 0;
		for(String key : metricCache.keySet()){
			Metric m = metricCache.get(key);
			
			long keyMaxTTLInMillis = (m.getTime() + cacheTimeoutInSecs) * 1000;
			
			//check if key was updated this round, if so then also keep it.
			if(m != null && m.isAlive()){ 
				newMap.put(key, m);
			}else if(keyMaxTTLInMillis > now){
				//key has not yet expired, keep around and put in put to persist
				newMap.put(key, m);
			}else{
				expired++;
			}
		}
		LOGGER.info("Found " + expired + " expired cache entries");
		//contains only the "live" metrics
		kryo.writeObject(output, newMap);
		
		LOGGER.info("Persist statistics cache with" + newMap.size() + " metrics");
	}

	public void load(Kryo kryo, Input input) {
		Map<String, Metric> metricCache = kryo.readObject(input, HashMap.class);
		for(String key : metricCache.keySet()){
			Metric m = metricCache.get(key);
			if(m != null){
				//mark metric as non-alive, so if not updated this run, then it will not be persisted again
				m.setAlive(false);
			}
		}
		LOGGER.info("Loaded statistics cache " + metricCache.size() + " metrics");
		metricManager.setMetricCache(metricCache);
	}

}

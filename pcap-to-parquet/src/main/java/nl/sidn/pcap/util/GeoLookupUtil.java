package nl.sidn.pcap.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.geoip.LookupService;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;

/**
 * Utility class to lookup IP adress informatie such as country and asn.
 * Uses the maxmind database
 */
public class GeoLookupUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoLookupUtil.class);
	
	private DatabaseReader reader;
	private LookupService asnService;
	private LookupService asnV6Service;
	
	
	public GeoLookupUtil() {
		Settings settings = Settings.getInstance();
		try {
			//geo
			File database = new File(settings.getSetting(Settings.MAXMIND_GEOIP_PATH));
			reader = new DatabaseReader.Builder(database).build();
			//asn
			asnService = new LookupService(settings.getSetting(Settings.MAXMIND_ASN_V4_PATH),
				    LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
			asnV6Service = new LookupService(settings.getSetting(Settings.MAXMIND_ASN_V6_PATH),
				    LookupService.GEOIP_MEMORY_CACHE | LookupService.GEOIP_CHECK_CACHE);
		} catch (IOException e) {
			throw new RuntimeException("Error initializing GEO/ASN database", e);
		}
	}
	
	public String lookupCountry(String ip){
		try {
			return lookupCountry(InetAddress.getByName(ip));
		} catch (UnknownHostException e) {
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug("No country found for: " + ip);
			}
			return null;
		}
	}
	
	public String lookupCountry(byte[] ip){
		try {
			return lookupCountry(InetAddress.getByAddress(ip));
		} catch (UnknownHostException e) {
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug("No country found for: " + ip);
			}
			return null;
		}
	}
	
	public String lookupCountry(InetAddress addr){
		CountryResponse response = null;
		
		try {
			response = reader.country(addr);
		} catch (Exception e) {
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug("No country found for: " + addr);
			}
			return null;
		}
		return response.getCountry().getIsoCode();
	}
	
	public String lookupCity(String ip){
		CityResponse response = null;
		
		try {
			response = reader.city(InetAddress.getByName(ip));
		} catch (Exception e) {
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug("No city found for: " + ip);
			}
			return null;
		}
	
		return response.getCity().getName();
	}
	
	public String lookupASN(byte[] ip, boolean v4){
		try {
			InetAddress addr = InetAddress.getByAddress(ip);
			return lookupASN(addr, v4);
		} catch (UnknownHostException e) {
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug("No asn found for: " + ip);
			}
			return null;
		}
	}
	
	public String lookupASN(InetAddress ip, boolean v4){
		String asn = null;
		try {
			if(v4){
				asn = asnService.getOrg(ip);
			}else{
				asn = asnV6Service.getOrgV6(ip);
			}
		} catch (Exception e) {
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug("No asn found for: " + ip);
			}
			return null;
		}
	
		return parseASN(asn);
	}
	
	public String lookupASN(String ip, boolean v4){
		String asn = null;
		try {
			if(v4){
				asn = asnService.getOrg(ip);
			}else{
				asn = asnV6Service.getOrgV6(ip);
			}
		} catch (Exception e) {
			if(LOGGER.isDebugEnabled()){
				LOGGER.debug("No country found for: " + ip);
			}
			return null;
		}
		return parseASN(asn);
	}
	
	/**
	 * parse maxmind asn result
	 * @return ASXXXX formatted string
	 */
	private String parseASN(String asn){
	
		if(asn != null){
			int pos = StringUtils.indexOf(asn, ' ');
			if(pos != -1){
				return StringUtils.substring(asn, 0, pos);
			}
			//not a valid asn string
			return "UNKN";
		}
		//not found
		return null;
	}

}

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
package nl.sidn.pcap.ip;

import java.net.UnknownHostException;

import nl.sidn.pcap.util.Settings;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * check if an IP address is a Google open resolver.
 * This check uses the list from the Google resolver website:
 * https://developers.google.com/speed/public-dns/faq
 * 
 * @author maarten
 * 
 */
public final class GoogleResolverCheck extends AbstractNetworkCheck {

	private static String GOOGLE_RESOLVER_IP_FILENAME = "google-resolvers";
	protected static final Logger LOGGER = LoggerFactory.getLogger(GoogleResolverCheck.class);
	
	@Override
	protected void init() {
		String url = Settings.getInstance().getSetting(Settings.RESOLVER_LIST_GOOGLE);
		LOGGER.info("Load Google resolver addresses from url: " + url );
		
		Document doc = null;
		try {
			doc = Jsoup.connect(url).get();
		} catch (Exception e) {
			LOGGER.error("Problem while getting Google resolvers url: " + url);
			return;
		}

		Elements tags = doc.getElementsByTag("pre");
		if(tags.size() == 2){
			Element resolvers = tags.get(0);
			//Element resolver = codes.get(0);
			String[] ips = StringUtils.split(resolvers.text(), '\n');
			for(String ip: ips){
				String[] parts = StringUtils.split(ip, ' ');
				if(parts.length == 2){
					if(LOGGER.isDebugEnabled()){
						LOGGER.debug("Add Google resolver IP range: " + parts[0]);
					}
					
					try {
						bit_subnets.add(Subnet.createInstance(parts[0]));
						subnets.add(parts[0]);
					} catch (UnknownHostException e) {
						LOGGER.error("Problem while adding Google resolver IP range: " + parts[0] + e);
					}
				}
			}
			
			if(subnets.size() == 0){
				LOGGER.error("No Google resolvers found at url: " + url);
			}
		}else{
			LOGGER.error("No Google resolvers found at url: " + url);
		}
	}

	@Override
	protected String getFilename() {
		return GOOGLE_RESOLVER_IP_FILENAME;
	}

}

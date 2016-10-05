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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UDF to check if an IP address is an OpenDNS resolver.
 * @author maarten
 *
 */
public final class OpenDNSResolverCheck extends AbstractNetworkCheck{
	
	private static String OPENDNS_RESOLVER_IP_FILENAME = "opendns-resolvers";
	protected static final Logger LOGGER = LoggerFactory.getLogger(GoogleResolverCheck.class);
	
	@Override
	protected void init() {
		String url = Settings.getInstance().getSetting(Settings.RESOLVER_LIST_OPENDNS);
		LOGGER.info("Load OpenDNS resolver addresses from url: " + url );
		
		Document doc = null;
		try {
			doc = Jsoup.connect(url).get();
		} catch (Exception e) {
			LOGGER.error("Problem while getting OpenDNS resolvers url: " + url);
			return;
		}

		Element networktable = doc.getElementById("networks");
		if(networktable != null){
			Elements rowElements = networktable.getElementsByTag("tr");
			if(rowElements.size() > 1){
				//skip table header
				for(int i=1; i<rowElements.size(); i++){
					Element row = rowElements.get(i);
					if(row.children().size() == 5){
						try {
							String ipv4 = row.child(3).text();
							String ipv6 = row.child(4).text();
							bit_subnets.add(Subnet.createInstance(ipv4));
							bit_subnets.add(Subnet.createInstance(ipv6));
							subnets.add(ipv4);
							subnets.add(ipv6);
						} catch (UnknownHostException e) {
							LOGGER.error("Problem while adding OpenDNS resolver IP range: " + row.children() + e);
						}
					}
				}
			}
		}
		if(subnets.size() == 0){
			LOGGER.error("No OpenDNS resolvers found at url: " + url);
		}
	}

	@Override
	protected String getFilename() {
		return OPENDNS_RESOLVER_IP_FILENAME;
	}

}

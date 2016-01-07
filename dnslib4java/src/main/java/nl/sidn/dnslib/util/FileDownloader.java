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
package nl.sidn.dnslib.util;

import java.util.Scanner;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

public class FileDownloader {
	
	private static final Logger LOGGER = Logger.getLogger(FileDownloader.class);

	
	@SuppressWarnings({ "resource" })
	public String download(String url){
		if(LOGGER.isDebugEnabled()){
			LOGGER.debug("download url: " + url);
		}
		HttpClient httpclient = null;
		try {
			httpclient = HttpClientBuilder.create().build();
			URIBuilder uriBuilder = new URIBuilder(url);
			uriBuilder.setParameter("http.useragent", "SIDN-DNS/1.0");
			HttpGet request = new HttpGet(uriBuilder.build());
			HttpResponse response = httpclient.execute(request);
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				StringBuilder builder = new StringBuilder();
				Scanner s = new Scanner(entity.getContent()).useDelimiter("\n");
				while (s.hasNext()) {
					builder.append(s.next() + "\n");
				}
				s.close();
			    return builder.toString();
			}
		} catch (Exception e) {
			throw new RuntimeException("Error while downloading url: " + url, e);
		}finally{
			HttpClientUtils.closeQuietly(httpclient);
		}
		
		return null;
	}

}

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

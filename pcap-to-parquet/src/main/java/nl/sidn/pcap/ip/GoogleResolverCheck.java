package nl.sidn.pcap.ip;

import java.io.IOException;
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
		} catch (IOException e) {
			LOGGER.error("Problem while getting Google resolvers url: " + url);
			return;
		}

		Elements codes = doc.getElementsByTag("code");
		if(codes.size() > 0){
			Element resolver = codes.get(0);
			String[] ips = StringUtils.split(resolver.text(), '\n');
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
		}else{
			LOGGER.error("No Google resolvers found at url: " + url);
		}
	}

	@Override
	protected String getFilename() {
		return GOOGLE_RESOLVER_IP_FILENAME;
	}

}

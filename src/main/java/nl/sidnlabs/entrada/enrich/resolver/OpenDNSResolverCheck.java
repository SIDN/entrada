/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidnlabs.entrada.enrich.resolver;

import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Checker to see if an IP address is an OpenDNS resolver. resolver ips are downloaded from opendns
 * website, url: https://www.opendns.com/network-map-data
 * 
 * @author maarten
 *
 */
@Log4j2
@Component
@Setter
public final class OpenDNSResolverCheck extends AbstractResolverCheck {

  private static final String RESOLVER_STATE_FILENAME = "opendns-resolvers";
  private static final String RESOLVER_NAME = "OpenDNS";


  @Value("${opendns.resolver.url}")
  private String url;
  @Value("${opendns.resolver.timeout:15}")
  private int timeout;

  @Override
  protected List<String> fetch() {
    log.info("Load OpenDNS resolver addresses from url: " + url);

    List<String> subnets = new ArrayList<>();

    RequestConfig config = RequestConfig
        .custom()
        .setConnectTimeout(timeout * 1000)
        .setConnectionRequestTimeout(timeout * 1000)
        .setSocketTimeout(timeout * 1000)
        .build();
    CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).useSystemProperties().build();

    try {
      HttpGet get = new HttpGet(url);
      CloseableHttpResponse response = client.execute(get);
      HttpEntity entity = response.getEntity();
      String html = EntityUtils.toString(entity);

      Document doc = Jsoup.parse(html);
      Elements tableRows = doc.select("table#networks tr");

      if (tableRows.size() > 0) {

        // skip table header row
        tableRows.stream().skip(1).forEach(r -> {
          Elements columns = r.getElementsByTag("td");

          String v4 = columns.get(3).text();
          String v6 = columns.get(4).text();

          if (log.isDebugEnabled()) {
            log
                .debug("Add OpenDNS resolver IP ranges -> location: " + columns.get(0).text()
                    + " subnetV4: " + v4 + " subnetV6: " + v6);
          }

          subnets.add(v4);
          subnets.add(v6);

        });

      }

    } catch (Exception e) {
      // ignore any problems when fetching resolver list
      log.error("Problem while adding OpenDns resolvers.", e);
    }
    return subnets;
  }

  @Override
  protected String getFilename() {
    return RESOLVER_STATE_FILENAME;
  }

  @Override
  public String getName() {
    return RESOLVER_NAME;
  }

}

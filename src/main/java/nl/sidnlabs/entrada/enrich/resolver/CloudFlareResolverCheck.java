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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import lombok.extern.log4j.Log4j2;

/**
 * check if an IP address is a Cloudflare open resolver.
 * 
 */
@Log4j2
@Component
public final class CloudFlareResolverCheck extends AbstractResolverCheck {

  private static final String RESOLVER_STATE_FILENAME = "cloudflare-resolvers";
  private static final String RESOLVER_NAME = "CloudFlare";

  @Value("${cloudflare.resolver.ipv4.url}")
  private String urlV4;
  @Value("${cloudflare.resolver.ipv6.url}")
  private String urlV6;
  @Value("${cloudflare.resolver.timeout:15}")
  private int timeout;

  @Override
  protected List<String> fetch() {

    int timeoutInMillis = timeout * 1000;
    RequestConfig config = RequestConfig
        .custom()
        .setConnectTimeout(timeoutInMillis)
        .setConnectionRequestTimeout(timeoutInMillis)
        .setSocketTimeout(timeoutInMillis)
        .build();
    CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).useSystemProperties().build();

    List<String> subnets = new ArrayList<>();
    process(client, urlV4, subnets);
    process(client, urlV6, subnets);

    return subnets;
  }


  private void process(CloseableHttpClient client, String url, List<String> subnets) {
    log.info("Fetch CloudFlare resolver addresses from url: " + url);

    HttpGet get = new HttpGet(url);
    try (CloseableHttpResponse response = client.execute(get)) {
      String content =
          StreamUtils.copyToString(response.getEntity().getContent(), StandardCharsets.UTF_8);

      if (log.isDebugEnabled()) {
        log.debug("content: {}", content);
      }

      String[] addresses = content.split("\n");
      for (String subnet : addresses) {
        if (log.isDebugEnabled()) {
          log.debug("Add CloudFlare resolver IP range: {}", subnet);
        }
        subnets.add(subnet);
      }
    } catch (Exception e) {
      log.error("Problem while adding CloudFlare resolvers for url: {}", url, e);
    }
  }


  @Override
  public String getFilename() {
    return RESOLVER_STATE_FILENAME;
  }

  @Override
  public String getName() {
    return RESOLVER_NAME;
  }

}

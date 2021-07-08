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
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.xbill.DNS.Cache;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.TXTRecord;
import org.xbill.DNS.Type;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * check if an IP address is a Google open resolver. This check uses the list from the Google
 * resolver network: dig TXT locations.publicdns.goog.
 * 
 * @author maarten
 * 
 */
@Log4j2
@Component
// @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Setter
public final class GoogleResolverCheck extends AbstractResolverCheck {

  private static final String RESOLVER_STATE_FILENAME = "google-resolvers";
  private static final String RESOLVER_NAME = "Google";

  @Value("${google.resolver.hostname}")
  public String hostname;
  @Value("${google.resolver.timeout:15}")
  private int timeout;


  @Override
  protected List<String> fetch() {

    try {
      Lookup l =
          new Lookup(StringUtils.endsWith(hostname, ".") ? hostname : hostname + ".", Type.TXT);
      // always make sure the cache is empty
      l.setCache(new Cache());
      Record[] records = l.run();
      if (records != null && records.length > 0) {
        return parse(records[0]);
      }
    } catch (Exception e) {
      log.error("Problem while adding Google resolvers, continue without", e);
    }

    log.error("No Google resolver addresses found");
    return Collections.emptyList();
  }

  private List<String> parse(Record record) {
    TXTRecord txt = (TXTRecord) record;
    List<String> subnets = new ArrayList<>();

    @SuppressWarnings("unchecked")
    List<String> lines = txt.getStrings();
    for (String line : lines) {
      String[] parts = StringUtils.split(line, " ");
      if (parts.length == 2) {
        if (log.isDebugEnabled()) {
          log.debug("Add Google resolver IP range: " + parts[0]);
        }
        subnets.add(parts[0]);
      }
    }

    if (subnets.isEmpty()) {
      log.error("No Google resolver addresses found");
    }

    return subnets;
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

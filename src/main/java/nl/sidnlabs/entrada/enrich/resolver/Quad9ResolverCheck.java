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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;


@Log4j2
@Component
public final class Quad9ResolverCheck extends AbstractResolverCheck {

  private static final String RESOLVER_SOURCE_FILENAME = "quad9-resolvers.txt";
  private static final String RESOLVER_STATE_FILENAME = "quad9-resolvers";
  private static final String RESOLVER_NAME = "Quad9";

  @Override
  protected List<String> fetch() {
    ClassPathResource resource = new ClassPathResource(RESOLVER_SOURCE_FILENAME);
    log.info("Load Quad9 resolver addresses from {}", resource);

    try (InputStream is = resource.getInputStream()) {
      List<String> lines = IOUtils.readLines(is, StandardCharsets.UTF_8);

      return lines
          .stream()
          .map(StringUtils::deleteWhitespace)
          .map(this::appendAddDefaultBits)
          .collect(Collectors.toList());

    } catch (Exception e) {
      log.error("Error while reading Quad9 data file");
    }

    return Collections.emptyList();
  }

  private String appendAddDefaultBits(String address) {
    if (!StringUtils.contains(address, "/")) {
      return address + "/32";
    }
    return address;
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

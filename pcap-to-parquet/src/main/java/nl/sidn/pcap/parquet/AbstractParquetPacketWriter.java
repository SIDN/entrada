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
package nl.sidn.pcap.parquet;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecordBuilder;
import lombok.extern.log4j.Log4j2;
import nl.sidn.pcap.exception.ApplicationException;
import nl.sidn.pcap.ip.geo.GeoIPService;
import nl.sidn.pcap.support.PacketCombination;

@Log4j2
public abstract class AbstractParquetPacketWriter {

  protected static final int STATUS_COUNT = 100000;

  protected int packetCounter;
  protected ParquetPartitionWriter writer;
  protected GeoIPService geoLookup;
  protected Set<String> countries = new HashSet<>();

  protected Schema avroSchema;


  public AbstractParquetPacketWriter(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;
  }

  protected Schema schema(String schema) {
    if (avroSchema != null) {
      return avroSchema;
    }

    String f = getClass().getClassLoader().getResource(schema).getFile();
    Parser parser = new Schema.Parser().setValidate(true);
    try {
      avroSchema = parser.parse(new File(f));
    } catch (IOException e) {
      throw new ApplicationException("Cannot load schema from file: " + f);
    }

    return avroSchema;
  }


  /**
   * use caching for maxmind otherwise cpu usage will be high and app will stall
   * 
   * @param lookup
   * @return
   */
  protected Optional<String> getCountry(String lookup) {
    Optional<String> country = geoLookup.lookupCountry(lookup);
    if (country.isPresent()) {
      countries.add(country.get());
    }
    return country;
  }

  protected Optional<String> getAsn(String lookup) {
    return geoLookup.lookupASN(lookup);
  }

  /**
   * create a parquet record which combines values from the query and the response
   * 
   * @param packet
   */
  public abstract void write(PacketCombination packet);

  public void open(String outputDir, String server, String name) {
    // replace any non alphanumeric chars in the servername with underscore
    String normalizedServer = server.replaceAll("[^A-Za-z0-9 ]", "_");
    String path = outputDir + System.getProperty("file.separator") + normalizedServer
        + System.getProperty("file.separator") + name;


    log.info("Create new Parquet writer with path: " + path);

    writer = new ParquetPartitionWriter(path);

    log.info("Created new Parquet writer");
  }

  /**
   * Create a new builder for every row.
   * 
   * @return
   */
  protected GenericRecordBuilder recordBuilder(String schema) {
    return new GenericRecordBuilder(schema(schema));
  }

  public void close() {
    showStatus();

    if (writer != null) {
      writer.close();
    }
  }

  protected void showStatus() {
    log.info(packetCounter + " packets written to parquet file.");
  }


  /**
   * replace all non printable ascii chars with the hex value of the char.
   * 
   * @param str
   * @return
   */
  public String filter(String str) {
    StringBuilder filtered = new StringBuilder(str.length());
    for (int i = 0; i < str.length(); i++) {
      char current = str.charAt(i);
      if (current >= 0x20 && current <= 0x7e) {
        filtered.append(current);
      } else {
        filtered.append("0x" + Integer.toHexString(current));
      }
    }

    return filtered.toString();
  }

  public abstract void writeMetrics();

  protected void updateMetricMap(Map<Integer, Integer> map, Integer key) {
    Integer currentVal = map.get(key);
    if (currentVal != null) {
      map.put(key, currentVal.intValue() + 1);
    } else {
      map.put(key, 1);
    }
  }

}


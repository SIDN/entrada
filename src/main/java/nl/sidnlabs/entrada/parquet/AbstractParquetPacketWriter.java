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
package nl.sidnlabs.entrada.parquet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.support.PacketCombination;

@Log4j2
public abstract class AbstractParquetPacketWriter {

  protected static final int STATUS_COUNT = 100000;

  protected int packetCounter;
  protected ParquetPartitionWriter writer;
  private List<AddressEnrichment> enrichments;
  protected Schema avroSchema;


  public AbstractParquetPacketWriter(List<AddressEnrichment> enrichments) {
    this.enrichments = enrichments;
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
   * @param schema schema to use
   * @return new GenericRecordBuilder for schema
   */
  protected GenericRecordBuilder recordBuilder(String schema) {
    return new GenericRecordBuilder(schema(schema));
  }

  protected void enrich(String address, String prefix, GenericRecordBuilder builder) {

    String cleanPrefix = StringUtils.trimToEmpty(prefix);

    // execute all enrichments and if a match is found add value to row
    enrichments.stream().filter(e -> e.match(address)).forEach(e -> {
      if (hasField(cleanPrefix + e.getColumn())) {
        builder.set(cleanPrefix + e.getColumn(), e.getValue());
      }
    });
  }

  private boolean hasField(String name) {
    return avroSchema.getField(name) != null;
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
   * @param str string to filter
   * @return filtered version of input string
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


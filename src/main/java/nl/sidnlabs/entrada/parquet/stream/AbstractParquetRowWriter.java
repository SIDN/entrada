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
package nl.sidnlabs.entrada.parquet.stream;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.load.RowWriter;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.parquet.ParquetPartitionWriter;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
public abstract class AbstractParquetRowWriter implements RowWriter {

  protected static final int STATUS_COUNT = 100000;

  protected boolean open;

  protected ServerContext serverCtx;
  protected int rowCounter;
  protected ParquetPartitionWriter writer;
  protected Schema avroSchema;
  protected int maxfilesize;
  protected int rowgroupsize;
  protected int pageRowLimit;

  @Value("${entrada.location.work}")
  protected String workLocation;

  protected Calendar cal;
  protected Partition partition;

  protected Set<Partition> partitions = new HashSet<>();

  public AbstractParquetRowWriter(int maxfilesize, int rowgroupsize, int pageRowLimit) {
    this.maxfilesize = maxfilesize;
    this.rowgroupsize = rowgroupsize;
    this.pageRowLimit = pageRowLimit;

    this.cal = Calendar.getInstance();
  }

  public Schema schema(String schema) {
    if (avroSchema != null) {
      // use cached version of schema
      return avroSchema;
    }

    try {
      Parser parser = new Schema.Parser().setValidate(true);
      avroSchema = parser.parse(new ClassPathResource(schema, getClass()).getInputStream());
    } catch (IOException e) {
      throw new ApplicationException("Cannot load schema from file: " + schema, e);
    }

    return avroSchema;
  }

  public void open(String outputDir, String server, String name) {
    String path = FileUtil.appendPath(outputDir, server, name);
    log.info("Create new Parquet writer using path: " + path);

    // make sure the path exists
    FileManager fm = FileManagerFactory.local();
    if (!fm.mkdir(path)) {
      log.error("Cannot create location: " + path);
    }

    writer = new ParquetPartitionWriter(path, maxfilesize, rowgroupsize, pageRowLimit);
    log.info("Created new Parquet writer using path: {}", path);

    open = true;
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

  protected boolean hasField(String name) {
    return avroSchema.getField(name) != null;
  }

  public void close() {
    log.info("Close " + type().name() + "Parquet writer");
    reset();

    if (writer != null) {
      writer.close();
    }

    open = false;
  }

  protected void showStatus() {
    log.info(rowCounter + " rows written to file.");
  }


  public void reset() {
    rowCounter = 0;
  }

  public Set<Partition> getPartitions() {
    return partitions;
  }

  @Override
  public void printStats() {
    log.info("------------------ " + type().name() + " Parquet Writer Stats ------------");
    log.info("Rows: {}", rowCounter);
  }

}


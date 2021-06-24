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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
import nl.sidnlabs.entrada.load.RowWriter;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
public abstract class AbstractParquetRowWriter implements RowWriter {

  protected static final int STATUS_COUNT = 100000;

  protected int rowCounter;
  protected ParquetPartitionWriter writer;
  protected Schema avroSchema;
  private int maxfilesize;
  private int rowgroupsize;
  private int pageRowLimit;

  public AbstractParquetRowWriter(int maxfilesize, int rowgroupsize, int pageRowLimit) {
    this.maxfilesize = maxfilesize;
    this.rowgroupsize = rowgroupsize;
    this.pageRowLimit = pageRowLimit;
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
      throw new ApplicationException("Cannot create location: " + path);
    }

    writer = new ParquetPartitionWriter(path, maxfilesize, rowgroupsize, pageRowLimit);
    log.info("Created new Parquet writer using path: {}", path);
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
    log.info("Close Parquet writer");
    showStatus();
    reset();

    if (writer != null) {
      writer.close();
    }
  }

  protected void showStatus() {
    log.info(rowCounter + " rows written to file.");
  }


  public void reset() {
    rowCounter = 0;
  }

}


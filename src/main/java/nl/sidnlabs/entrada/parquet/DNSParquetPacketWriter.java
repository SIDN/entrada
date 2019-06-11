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

import java.util.Calendar;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.model.Row;

@Component
public class DNSParquetPacketWriter extends AbstractParquetPacketWriter {

  private static final String DNS_AVRO_SCHEMA = "dns-query.avsc";

  public DNSParquetPacketWriter(@Value("${entrada.parquet.max:3000000}") int maxRows) {
    super(maxRows);
  }

  /**
   * create 1 parquet record which combines values from the query and the response
   *
   * @param row row to write row to Parquet formatted file
   * @param servers the name server the row is linked to
   */
  @Override
  public void write(Row row, String server) {

    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(row.getTs().getTime());

    // convert to avro
    GenericRecordBuilder builder = recordBuilder(DNS_AVRO_SCHEMA);

    // map all the columns in the row to the avro record fields
    row.getColumns().stream().forEach(c -> {
      if (hasField(c.getName())) {
        builder.set(c.getName(), c.getValue());
      }
    });

    // create the actual record and write to parquet file
    GenericRecord record = builder.build();

    writer
        .write(record, schema(DNS_AVRO_SCHEMA), cal.get(Calendar.YEAR), cal.get(Calendar.MONTH),
            cal.get(Calendar.DAY_OF_MONTH), server);
  }
}


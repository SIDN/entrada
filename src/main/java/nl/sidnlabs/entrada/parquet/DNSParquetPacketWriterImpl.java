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
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.Row;

@Component("parquet-dns")
public class DNSParquetPacketWriterImpl extends AbstractParquetRowWriter {

  private static final String DNS_AVRO_SCHEMA = "/avro/dns-query.avsc";


  public DNSParquetPacketWriterImpl(
      @Value("#{${entrada.parquet.filesize.max:128}*1024*1024}") int maxfilesize,
      @Value("#{${entrada.parquet.rowgroup.size:128}*1024*1024}") int rowgroupsize) {
    super(maxfilesize, rowgroupsize);
  }

  /**
   * create 1 parquet record which combines values from the query and the response
   *
   * @param row row to write row to Parquet formatted file
   * @param server the name server the row is linked to
   */
  @Override
  public Partition write(Row row, String server) {
    rowCounter++;

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
    Partition partition = Partition
        .builder()
        .year(cal.get(Calendar.YEAR))
        .month(cal.get(Calendar.MONTH) + 1)
        .day(cal.get(Calendar.DAY_OF_MONTH))
        .server(server)
        .dns(true)
        .build();

    writer.write(record, schema(DNS_AVRO_SCHEMA), partition);

    return partition;
  }
}


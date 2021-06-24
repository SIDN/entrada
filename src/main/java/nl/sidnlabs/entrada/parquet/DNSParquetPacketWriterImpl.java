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
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.Row;

@Component("parquet-dns")
public class DNSParquetPacketWriterImpl extends AbstractParquetRowWriter {

  private static final String DNS_AVRO_SCHEMA = "/avro/dns-query.avsc";

  private Calendar cal = Calendar.getInstance();
  private GenericRecordBuilder builder = recordBuilder(DNS_AVRO_SCHEMA);
  private List<Field> fields;
  private Partition partition;

  public DNSParquetPacketWriterImpl(
      @Value("#{${entrada.parquet.filesize.max:128}*1024*1024}") int maxfilesize,
      @Value("#{${entrada.parquet.rowgroup.size:128}*1024*1024}") int rowgroupsize,
      @Value("${entrada.parquet.page-row.limit:20000}") int pageRowLimit) {
    super(maxfilesize, rowgroupsize, pageRowLimit);

    this.cal = Calendar.getInstance();
    this.builder = recordBuilder(DNS_AVRO_SCHEMA);
    this.fields = schema(DNS_AVRO_SCHEMA).getFields();
  }

  /**
   * create 1 parquet record which combines values from the query and the response
   * 
   * @param row row to write row to Parquet formatted file
   * @param server the name server the row is linked to
   */
  @Override
  public Partition write(Row row, String server) {
    // NOTE: make sure not to do any expensive stuff here, this method is called
    // many times. cache stuff where possible
    rowCounter++;
    // use time from row for parquet row
    cal.setTimeInMillis(row.getTs().getTime());

    // reuse old builder, first clear old values
    for (Field f : fields) {
      builder.clear(f);
    }

    // map all the columns in the row to the avro record fields
    row.getColumns().stream().forEach(c -> {
      builder.set(c.getName(), c.getValue());
    });

    // create the actual record and write to parquet file
    GenericRecord record = builder.build();
    // try to reuse the partition instance if possible
    if (partition == null || partition.getDay() != cal.get(Calendar.DAY_OF_MONTH)
        || partition.getMonth() != cal.get(Calendar.MONTH) + 1
        || partition.getYear() != cal.get(Calendar.YEAR)
        || !StringUtils.equals(partition.getServer(), server)) {

      partition = Partition
          .builder()
          .year(cal.get(Calendar.YEAR))
          .month(cal.get(Calendar.MONTH) + 1)
          .day(cal.get(Calendar.DAY_OF_MONTH))
          .server(server)
          .dns(true)
          .build();

    }
    writer.write(record, schema(DNS_AVRO_SCHEMA), partition);

    return partition;
  }
}


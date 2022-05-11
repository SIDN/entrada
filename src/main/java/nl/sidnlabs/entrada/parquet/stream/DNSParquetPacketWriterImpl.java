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

import java.util.Calendar;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.model.ProtocolType;

@Component("parquet-dns")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DNSParquetPacketWriterImpl extends AbstractParquetRowWriter {

  private static final String DNS_AVRO_SCHEMA = "/avro/dns-query.avsc";
  private Schema schema = schema(DNS_AVRO_SCHEMA);
  private final MeterRegistry registry;

  public DNSParquetPacketWriterImpl(
          @Value("#{${entrada.parquet.filesize.max:128}*1024*1024}") int maxfilesize,
          @Value("#{${entrada.parquet.rowgroup.size:128}*1024*1024}") int rowgroupsize,
          @Value("${entrada.parquet.page-row.limit:20000}") int pageRowLimit, ServerContext ctx, MeterRegistry registry) {
    super(maxfilesize, rowgroupsize, pageRowLimit, ctx);
    this.registry = registry;
  }

  /**
   * create 1 parquet record which combines values from the query and the response
   * 
   * @param row row to write row to Parquet formatted file
   * @param server the name server the row is linked to
   */
  @Override
  public void write(GenericRecord record, String server) {
    // NOTE: make sure not to do any expensive work here, this method is called
    // many times. cache stuff where possible
    // thats why we are not using the Avro GenericRecordBuilder

    if (writer == null) {
      open();
    }

    rowCounter++;
    if (rowCounter % STATUS_COUNT == 0) {
      showStatus();
    }

    // use time from row for parquet row
    cal.setTimeInMillis(((Long) record.get("time")).longValue());

    // try to reuse the partition instance if possible
    if (isNewPartition(server)) {
      partition = createPartition(server);
      partitions.add(partition);
    }

    writer.write(record, schema, partition);
    registry.counter("entrada.dns.packets.processed").increment();
  }

  private boolean isNewPartition(String server) {
    return partition == null || partition.getDay() != cal.get(Calendar.DAY_OF_MONTH)
        || partition.getMonth() != cal.get(Calendar.MONTH) + 1
        || partition.getYear() != cal.get(Calendar.YEAR)
        || !StringUtils.equals(partition.getServer(), server);
  }

  private Partition createPartition(String server) {
    return Partition
        .builder()
        .year(cal.get(Calendar.YEAR))
        .month(cal.get(Calendar.MONTH) + 1)
        .day(cal.get(Calendar.DAY_OF_MONTH))
        .server(server)
        .dns(true)
        .build();

  }

  @Override
  public ProtocolType type() {
    return ProtocolType.DNS;
  }

}


package nl.sidnlabs.pcap.parquet;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ParquetPartitionWriter {

  private String path;
  private Map<String, ParquetPartition<GenericRecord>> partitions = new HashMap<>();

  public ParquetPartitionWriter(String path) {
    this.path = path;
  }

  public void write(GenericRecord rec, Schema schema, int year, int month, int day) {
    write(rec, schema, year, month, day, null);
  }

  public void write(GenericRecord rec, Schema schema, int year, int month, int day, String server) {

    // check is partition already exists, if not create a new partition
    String partition = ParquetPartition.partition(path, year, month, day, server);
    ParquetPartition<GenericRecord> parquetPartition =
        partitions.computeIfAbsent(partition, k -> new ParquetPartition<>(partition, schema));

    // write the rec to the partition
    parquetPartition.write(rec);

  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close {} partitions ", partitions.size());
    }
    partitions.entrySet().stream().forEach(entry -> entry.getValue().close());
  }
}

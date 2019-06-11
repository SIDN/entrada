package nl.sidnlabs.entrada.parquet;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Value
public class ParquetPartitionWriter {

  private String path;
  // default max 3 million packets per files max (+/- 125mb files)
  private int maxRows;

  private Map<String, ParquetPartition<GenericRecord>> partitions = new HashMap<>();

  public ParquetPartitionWriter(String path, int maxRows) {
    this.path = path;
    this.maxRows = maxRows;
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

    // check if size of parquet partition is too big
    if (parquetPartition.getRows() >= maxRows) {
      log
          .info(
              "Max DNS packets reached for this Parquet parition {}, close current file and create new",
              partition);

      parquetPartition.close();
      // remove partition from partitions map, for a possible next row fot this partitions
      // a new partition object and parquet file will be created.
      partitions.remove(partition);
    }

  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close {} partitions ", partitions.size());
    }
    partitions.entrySet().stream().forEach(entry -> entry.getValue().close());
  }

}

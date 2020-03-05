package nl.sidnlabs.entrada.parquet;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.util.FileUtil;

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

  public void write(GenericRecord rec, Schema schema, Partition partition) {

    String partitionStr = FileUtil.appendPath(path, partition.toPath());
    // check is partition already exists, if not create a new partition
    ParquetPartition<GenericRecord> parquetPartition =
        partitions.computeIfAbsent(partitionStr, k -> new ParquetPartition<>(partitionStr, schema));

    // write the rec to the partition
    parquetPartition.write(rec);

    // check if size of parquet partition is too big
    if (parquetPartition.getRows() >= maxRows) {
      log
          .info(
              "Max DNS packets reached for this Parquet parition {}, close current file and create new",
              partitionStr);

      parquetPartition.close();
      // remove partition from partitions map, for a possible next row for this partitions
      // a new partition object and parquet file will be created.
      partitions.remove(partitionStr);
    }
  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close {} partitions ", partitions.size());
    }

    // close writers, let them write all data to disk
    partitions.entrySet().stream().forEach(entry -> entry.getValue().close());

    // make sure to clear the partitions map, otherwise the parquetwriters will remain in memory and
    // gc will not be able to free the memory
    partitions.clear();
  }

}

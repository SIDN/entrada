package nl.sidnlabs.entrada.parquet;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@Getter
@Setter
public class ParquetPartitionWriter {

  private final static int MAX_SIZE_CHECK = 10000;

  private String path;
  private int maxSize;
  private int rowgroupsize;
  private int pageRowLimit;
  private int rows = 0;

  private Map<String, ParquetPartition<GenericRecord>> partitions = new HashMap<>();

  public ParquetPartitionWriter(String path, int maxSize, int rowgroupsize, int pageRowLimit) {
    this.path = path;
    this.maxSize = maxSize;
    this.rowgroupsize = Math.min(maxSize, rowgroupsize);
    this.pageRowLimit = pageRowLimit;
  }

  public boolean write(GenericRecord rec, Schema schema, Partition partition) {
    rows++;

    boolean newPartition = false;
    String partitionStr = FileUtil.appendPath(path, partition.toPath());
    // check is partition already exists, if not create a new partition
    ParquetPartition<GenericRecord> parquetPartition = partitions.get(partitionStr);
    if (parquetPartition == null) {
      parquetPartition = new ParquetPartition<>(partitionStr, schema, rowgroupsize, pageRowLimit);
      partitions.put(partitionStr, parquetPartition);
      newPartition = true;
    }

    // write the rec to the partition
    parquetPartition.write(rec);

    // check if size of parquet partition is too big
    // do this every MAX_SIZE_CHECK rows, to keep overhead low
    if (rows % MAX_SIZE_CHECK == 0) {
      long size = parquetPartition.size();
      if (size >= maxSize) {
        log
            .info("Partition {} size {} > max ({}), close current file and create new",
                partitionStr, size, maxSize);

        parquetPartition.close();
        // remove partition from partitions map, for a possible next row for this partitions
        // a new partition object and parquet file will be created.
        partitions.remove(partitionStr);
      }
    }

    return newPartition;
  }

  public void close() {
    log.info("close {} partitions ", partitions.size());

    // close writers, let them write all data to disk
    partitions.entrySet().stream().forEach(entry -> entry.getValue().close());

    // make sure to clear the partitions map, otherwise the parquetwriters will remain in memory and
    // gc will not be able to free the memory
    partitions.clear();
  }

}

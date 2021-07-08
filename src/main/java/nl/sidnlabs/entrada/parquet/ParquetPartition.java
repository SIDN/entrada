package nl.sidnlabs.entrada.parquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
@Value
public class ParquetPartition<T> {

  private static Configuration conf = new Configuration();

  private ParquetWriter<T> writer;
  private String filename;
  @NonFinal
  private int rows = 0;
  private Path currentFile;

  public ParquetPartition(String partition, Schema schema, int rowgroupsize, int pageRowLimit) {

    long start = System.currentTimeMillis();

    currentFile = new Path(
        partition + System.getProperty("file.separator") + UUID.randomUUID() + ".parquet.active");
    filename = currentFile.toString();

    log.info("Create new parquet file: {}", filename);

    try {
      Files.createDirectories(Paths.get(partition));

      writer = AvroParquetWriter
          .<T>builder(currentFile)
          .enableDictionaryEncoding()
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withConf(conf)
          .withWriterVersion(WriterVersion.PARQUET_1_0)
          .withSchema(schema)
          .withRowGroupSize(rowgroupsize)
          .withPageRowCountLimit(pageRowLimit)
          .build();

      log.info("Created ParquetWriter, time: {}", System.currentTimeMillis() - start);

    } catch (IOException e) {
      throw new ApplicationException("Cannot create a Parquet parition", e);
    }
  }

  public void write(T data) {
    try {
      writer.write(data);
      rows++;
    } catch (Exception e) {
      // cannot write this row, log error and continue
      // do not stop the writer
      log.error("Error writing row to parquet", e);
    }
  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close file: {}", filename);
    }
    try {
      writer.close();
    } catch (IOException e) {
      // cannot close this writer, log error and continue
      // do not stop the writer
      log.error("Cannot close file: " + filename, e);
    }

    if (StringUtils.endsWith(filename, ".active")) {
      // rename files ending with .parquet.active to .parquet
      // to indicate that they are no longer being written to
      java.nio.file.Path source = Paths.get(filename);
      java.nio.file.Path target = Paths.get(StringUtils.removeEnd(filename, ".active"));

      try {
        Files.move(source, target);
      } catch (Exception e) {
        log.error("Cannot move file: {} to: {}" + source, target, e);
      }
    }
  }

  public long size() {
    return writer != null ? writer.getDataSize() : 0;
  }
}

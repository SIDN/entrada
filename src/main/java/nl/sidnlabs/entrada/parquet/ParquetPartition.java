package nl.sidnlabs.entrada.parquet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.avro.Schema;
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

  private ParquetWriter<T> writer;
  private String filename;
  @NonFinal
  private int rows = 0;

  public ParquetPartition(String partition, Schema schema) {

    Configuration conf = new Configuration();
    Path file =
        new Path(partition + System.getProperty("file.separator") + UUID.randomUUID() + ".parquet");
    filename = file.toString();

    log.info("Create new parquet file: {}", filename);

    try {
      Files.createDirectories(Paths.get(partition));

      writer = AvroParquetWriter
          .<T>builder(file)
          .enableDictionaryEncoding()
          .enableValidation()
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withConf(conf)
          .withWriterVersion(WriterVersion.PARQUET_1_0)
          .withSchema(schema)
          .build();
    } catch (IOException e) {
      throw new ApplicationException("Cannot create a Parquet parition", e);
    }
  }

  public void write(T data) {
    try {
      writer.write(data);
      rows++;
    } catch (Exception e) {
      throw new ApplicationException("Cannot write data", e);
    }
  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close()");
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new ApplicationException("Cannot close file: " + filename, e);
    }
  }
}

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
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
public class ParquetPartition<T> {

  private static final String FILE_SEP = System.getProperty("file.separator");
  private ParquetWriter<T> writer;
  private Path file;

  public static String partition(String path, int year, int month, int day, String server) {
    String partition =
        path + FILE_SEP + "year=" + year + FILE_SEP + "month=" + month + FILE_SEP + "day=" + day;

    if (StringUtils.isNotBlank(server)) {
      partition = partition + FILE_SEP + "server=" + server;
    }

    return partition;
  }

  public ParquetPartition(String partition, Schema schema) {
    Configuration conf = new Configuration();
    file = new Path(partition + FILE_SEP + UUID.randomUUID() + ".parquet");

    try {
      Files.createDirectories(Paths.get(partition));

      writer = AvroParquetWriter
          .<T>builder(file)
          .enableDictionaryEncoding()
          .enableValidation()
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withConf(conf)
          .withWriterVersion(WriterVersion.PARQUET_2_0)
          .withSchema(schema)
          .build();
    } catch (IOException e) {
      throw new ApplicationException("Cannot create a Parquet parition", e);
    }
  }

  public void write(T data) {
    try {
      writer.write(data);
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
      throw new ApplicationException("Cannot close file: " + file, e);
    }
  }

}

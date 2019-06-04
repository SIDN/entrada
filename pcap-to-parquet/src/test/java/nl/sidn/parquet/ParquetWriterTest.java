package nl.sidn.parquet;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;
import nl.sidn.pcap.parquet.ParquetPartitionWriter;

public class ParquetWriterTest {


  @Test
  public void test() throws Exception {
    ParquetPartitionWriter writer = new ParquetPartitionWriter(
        "/Users/maartenwullink/sidn/development/git/entrada/pcap-to-parquet/target/parquet/");

    Parser parser = new Schema.Parser().setValidate(true);
    Schema avroSchema = parser.parse(new File("src/test/resources/dns-query.avsc"));

    for (int i = 0; i < 10; i++) {



      GenericData.Record record = new GenericData.Record(avroSchema);

      record.put("id", 0);
      record.put("unixtime", 0);
      record.put("time", 0);
      record.put("ipv", 0);
      record.put("prot", 0);
      record.put("src", "src");
      record.put("dst", "dst");
      record.put("dstp", 0);
      record.put("svr", "svr");
      record.put("qdcount", 0);
      record.put("opcode", 0);
      record.put("rcode", 0);
      record.put("labels", 0);
      record.put("time_micro", 0);
      record.put("res_len", 0);
      record.put("is_google", false);
      record.put("is_opendns", true);
      record.put("domainname", "sidn.nl");
      record.put("edns_padding", 12);

      // writer.write(rec, year, month, day, server);
      // writer.write(record);
      writer.write(record, 2019, 5, i, "myserver");
    }

    writer.close();
  }


  @Test
  public void test2() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/my-data.parquet");

    Parser parser = new Schema.Parser().setValidate(true);
    Schema avroSchema = parser.parse(new File("src/test/resources/dns-query.avsc"));

    // GenericRecordBuilder recBuilder = new GenericRecordBuilder(avroSchema);


    ParquetWriter<GenericData.Record> writer =
        AvroParquetWriter.<GenericData.Record>builder(root).enableDictionaryEncoding()
            .enableValidation().withCompressionCodec(CompressionCodecName.SNAPPY).withConf(conf)
            .withWriterVersion(WriterVersion.PARQUET_1_0).withSchema(avroSchema).build();

    for (int i = 0; i < 1000; i++) {


      GenericData.Record record = new GenericData.Record(avroSchema);

      record.put("id", 0);
      record.put("unixtime", 0);
      record.put("time", 0);
      record.put("ipv", 0);
      record.put("prot", 0);
      record.put("src", "src");
      record.put("dst", "dst");
      record.put("dstp", 0);
      record.put("svr", "svr");
      record.put("qdcount", 0);
      record.put("opcode", 0);
      record.put("rcode", 0);
      record.put("labels", 0);
      record.put("time_micro", 0);
      record.put("res_len", 0);
      record.put("is_google", false);
      record.put("is_opendns", true);
      record.put("domainname", "sidn.nl");
      record.put("edns_padding", 12);

      writer.write(record);
    }

    writer.close();
  }

}

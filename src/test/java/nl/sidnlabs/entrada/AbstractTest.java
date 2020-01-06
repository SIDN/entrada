package nl.sidnlabs.entrada;

import java.io.DataInputStream;
import java.io.InputStream;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.util.CompressionUtil;
import nl.sidnlabs.pcap.PcapReader;

@Log4j2
public abstract class AbstractTest {

  protected PcapReader createReaderFor(String file) {
    ClassPathResource resource = new ClassPathResource(file);
    log.info("Load pcap from {}", resource);

    try {
      DataInputStream is = new DataInputStream(
          CompressionUtil.getDecompressorStreamWrapper(resource.getInputStream(), 8 * 1024, file));
      return new PcapReader(is);
    } catch (Exception e) {
      log.error("Error while reading file", e);
    }

    throw new RuntimeException("Cannot create reader for file: " + file);
  }

  protected PcapReader createReaderForComppressed(String file) {
    ClassPathResource resource = new ClassPathResource(file);
    log.info("Load pcap from {}", resource);

    try {
      InputStream decompressor =
          CompressionUtil.getDecompressorStreamWrapper(resource.getInputStream(), 8 * 1024, file);

      DataInputStream dis = new DataInputStream(decompressor);
      return new PcapReader(dis);
    } catch (Exception e) {
      log.error("Error while reading file", e);
    }

    throw new RuntimeException("Cannot create reader for file: " + file);
  }

}

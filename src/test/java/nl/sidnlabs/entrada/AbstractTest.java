package nl.sidnlabs.entrada;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.pcap.PcapReader;

@Log4j2
public class AbstractTest {

  protected PcapReader createReaderFor(String file) {
    ClassPathResource resource = new ClassPathResource(file);
    log.info("Load pcap from {}", resource);

    try (InputStream is = resource.getInputStream()) {
      DataInputStream dis = new DataInputStream(new BufferedInputStream(is, 64 * 1024));
      return new PcapReader(dis);
    } catch (Exception e) {
      log.error("Error while reading file", e);
    }

    throw new RuntimeException("Cannot create reader for file: " + file);
  }

}

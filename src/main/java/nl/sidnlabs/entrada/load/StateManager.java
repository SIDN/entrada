package nl.sidnlabs.entrada.load;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
@Component
public class StateManager {

  private static final String DECODER_STATE_FILE = "pcap-decoder-state";
  private static final Kryo KRYO = new Kryo();

  private Output output = null;
  private Input input = null;

  @Value("${entrada.location.persistence}")
  private String workLocation;

  private ServerContext settings;

  public StateManager(ServerContext settings) {
    this.settings = settings;

    KRYO.setRegistrationRequired(false);
  }

  private String createStateFileName() {
    return workLocation + "/" + DECODER_STATE_FILE + "-" + settings.getServerInfo().getName()
        + ".bin";
  }

  public void write(Object data) {
    if (output == null) {
      String f = createStateFileName();
      if (log.isDebugEnabled()) {
        log.debug("Create KRYO output linked to file {}", f);
      }
      try {
        output = new Output(new FileOutputStream(f));
      } catch (Exception e) {
        throw new ApplicationException("Cannot create state file: " + f, e);
      }
    }

    KRYO.writeClassAndObject(output, data);
  }

  public Object read() {
    if (input == null) {
      String f = createStateFileName();
      if (log.isDebugEnabled()) {
        log.debug("Create KRYO input linked to file {}", f);
      }
      try {
        input = new Input(new FileInputStream(f));
      } catch (Exception e) {
        throw new ApplicationException("Cannot read state file: " + f, e);
      }
    }

    return KRYO.readClassAndObject(input);
  }

  public boolean stateAvailable() {
    return new File(createStateFileName()).exists();
  }

  public void delete() {
    try {
      Files.deleteIfExists(Paths.get(createStateFileName()));
    } catch (IOException e) {
      log.error("Error while trying to delete file: {}", createStateFileName(), e);
    }
  }

  public void close() {
    if (input != null) {
      input.close();
      input = null;
    }

    if (output != null) {
      output.flush();
      output.close();
      output = null;
    }
  }

}

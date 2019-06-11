package nl.sidnlabs.entrada.load;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.config.Settings;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
@Component
public class PersistenceManager {

  private static final String DECODER_STATE_FILE = "pcap-decoder-state";
  private static final Kryo KRYO = new Kryo();

  private Output output = null;
  private Input input = null;

  @Value("${entrada.location.work}")
  private String workLocation;

  private String createStateFileName() {
    return workLocation + "/" + DECODER_STATE_FILE + "-" + Settings.getServerInfo().getFullname()
        + ".bin";
  }

  public void write(Object data) {
    if (output == null) {
      String f = createStateFileName();
      try {
        log.info("Create KRYO output linked to file {}", f);
        output = new Output(new FileOutputStream(f));
      } catch (FileNotFoundException e) {
        throw new ApplicationException("Cannot create state file: " + f, e);
      }
    }

    KRYO.writeObject(output, data);
  }

  public <T> T read(Class<T> type) {
    if (input == null) {
      String f = createStateFileName();
      try {
        log.info("Create KRYO input linked to file {}", f);
        input = new Input(new FileInputStream(f));
      } catch (FileNotFoundException e) {
        throw new ApplicationException("Cannot read state file: " + f, e);
      }
    }

    return KRYO.readObject(input, type);
  }

  public boolean stateAvailable() {
    return new File(createStateFileName()).exists();
  }

  public void close() {
    if (input != null) {
      input.close();
      input = null;
    }

    if (output != null) {
      output.close();
      output = null;
    }
  }

}

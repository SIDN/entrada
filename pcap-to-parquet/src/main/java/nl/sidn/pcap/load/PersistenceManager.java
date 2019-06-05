package nl.sidn.pcap.load;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import org.springframework.stereotype.Component;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.log4j.Log4j2;
import nl.sidn.pcap.config.Settings;
import nl.sidn.pcap.exception.ApplicationException;

@Log4j2
@Component
public class PersistenceManager {

  private static final String DECODER_STATE_FILE = "pcap-decoder-state";
  private static final Kryo KRYO = new Kryo();

  private Output output = null;
  private Input input = null;
  private Settings settings;

  public PersistenceManager(Settings settings) {
    this.settings = settings;
  }

  private String createStateFileName() {
    return settings.getStateDir() + "/" + DECODER_STATE_FILE + "-"
        + settings.getServerInfo().getFullname() + ".bin";
  }

  public void write(Object data) {
    if (output == null) {
      try {
        output = new Output(new FileOutputStream(createStateFileName()));
      } catch (FileNotFoundException e) {
        throw new ApplicationException("Cannot create state file: " + createStateFileName(), e);
      }
    }

    KRYO.writeObject(output, data);
  }

  public <T> T read(Class<T> type) {
    if (input == null) {
      try {
        input = new Input(new FileInputStream(createStateFileName()));
      } catch (FileNotFoundException e) {
        throw new ApplicationException("Cannot read state file: " + createStateFileName(), e);
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

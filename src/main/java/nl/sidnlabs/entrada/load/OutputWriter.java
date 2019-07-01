package nl.sidnlabs.entrada.load;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import nl.sidnlabs.entrada.model.Partition;
import nl.sidnlabs.entrada.support.RowData;

/**
 * 
 * For every output format e.g. Parquet, JSON, a writer class must be created that implements this
 * interface.
 *
 */
public interface OutputWriter {

  /**
   * Start the Writer by giving it a queue from where it will receive rows to transform to output
   * format. This method should be execute by a separate thread to be able to read and write rows
   * simultaneously
   * 
   * @param dns enable dns output
   * @param icmp enable icmp output
   * @param input the input queue from where to read new rows.
   * @return a Future which the caller must check too find out if the writer has finished
   */
  Future<Map<String, Set<Partition>>> start(boolean dns, boolean icmp,
      LinkedBlockingQueue<RowData> input);

}

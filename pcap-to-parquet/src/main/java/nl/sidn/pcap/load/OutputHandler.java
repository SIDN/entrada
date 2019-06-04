package nl.sidn.pcap.load;

import nl.sidn.pcap.support.PacketCombination;

/**
 * 
 * Fot every output format e.g. Parquet, JSON, a handler must be created that implements this
 * interface.
 *
 */
public interface OutputHandler {

  void handle(PacketCombination p);

}

package nl.sidnlabs.entrada.load;

import nl.sidnlabs.entrada.support.PacketCombination;

/**
 * 
 * Fot every output format e.g. Parquet, JSON, a handler must be created that implements this
 * interface.
 *
 */
public interface OutputWriter {

  void write(PacketCombination p);

}

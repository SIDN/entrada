package nl.sidnlabs.entrada.model;

import nl.sidnlabs.entrada.support.PacketCombination;

public interface RowBuilder {

  Row build(PacketCombination combo);

  void writeMetrics();
}

package nl.sidnlabs.entrada.model;

import nl.sidnlabs.entrada.support.RowData;

public interface RowBuilder {

  Row build(RowData combo);

  void reset();

}

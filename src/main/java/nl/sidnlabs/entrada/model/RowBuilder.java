package nl.sidnlabs.entrada.model;

import java.util.List;
import akka.japi.Pair;
import nl.sidnlabs.entrada.support.RowData;

public interface RowBuilder {

  Pair<Row, List> build(RowData combo, String server);

  void reset();

}

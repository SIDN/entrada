package nl.sidnlabs.entrada.model;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Row {

  private ProtocolType type;
  private long time;

  private List<Column<?>> columns = new ArrayList<>(100);

  public Row(ProtocolType type, long time) {
    this.type = type;
    this.time = time;
  }

  public Row addColumn(Column<?> col) {
    columns.add(col);
    return this;
  }

  @Getter
  @Setter
  @AllArgsConstructor
  public static class Column<T> {
    private String name;
    private T value;
  }

}

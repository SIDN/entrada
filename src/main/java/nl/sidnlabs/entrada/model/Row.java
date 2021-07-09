package nl.sidnlabs.entrada.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.ToString;
import lombok.Value;

@Data
public class Row {

  private ProtocolType type;
  private long time;

  @ToString.Exclude
  private List<Column<?>> columns = new ArrayList<>(100);

  public Row(ProtocolType type, long time) {
    this.type = type;
    this.time = time;
  }

  public Row addColumn(Column<?> col) {
    columns.add(col);
    return this;
  }

  @Value
  public static class Column<T> {
    private String name;
    private T value;
  }

}

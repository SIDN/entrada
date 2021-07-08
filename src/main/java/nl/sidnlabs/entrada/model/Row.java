package nl.sidnlabs.entrada.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.ToString;
import lombok.Value;
import nl.sidnlabs.entrada.metric.Metric;

@Data
public class Row {

  private ProtocolType type;
  private long time;

  @ToString.Exclude
  private List<Column<?>> columns = new ArrayList<>(100);
  @ToString.Exclude
  private List<Metric> metrics = new ArrayList<>(10);

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

package nl.sidnlabs.entrada.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import lombok.ToString;
import lombok.Value;

@Value
public class Row {

  private Timestamp ts;

  @ToString.Exclude
  private List<Column<?>> columns = new ArrayList<>();

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

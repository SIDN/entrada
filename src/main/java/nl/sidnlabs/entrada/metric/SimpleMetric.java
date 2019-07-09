package nl.sidnlabs.entrada.metric;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class SimpleMetric implements Metric {

  private String name;
  private int value;
  private long time;


  public SimpleMetric(String name, int value, long time) {
    this.name = name;
    this.value = value;
    this.time = time;
  }

  public void update(int value) {
    this.value += value;
  }

  @Override
  public String toString() {
    return name + " " + value + " " + time;
  }
}

package nl.sidnlabs.entrada.metric;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CounterMetric extends AbstractMetric {

  public CounterMetric(String name, int value, long time) {
    super(name, value, time);
  }

}

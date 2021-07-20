package nl.sidnlabs.entrada.metric;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class SimpleMetric extends AbstractMetric {

  public SimpleMetric(String name, int value, long time) {
    super(name, value, time);
  }

}

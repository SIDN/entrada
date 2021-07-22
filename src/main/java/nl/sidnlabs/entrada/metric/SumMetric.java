package nl.sidnlabs.entrada.metric;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class SumMetric implements Metric {

  protected String name;
  protected double value;
  protected int samples;
  protected long time;
  protected boolean cached;
  protected boolean updated;


  public SumMetric(String name, int value, long time) {
    this.name = name;
    this.time = time;
    update(value);
  }

  public void update(int value) {
    this.value += value;
    samples++;
    updated = true;
  }

  @Override
  public String toString() {
    return name + " " + value + " " + time;
  }

  @Override
  public int getSamples() {
    return samples;
  }

  @Override
  public boolean isCached() {
    return cached;
  }

  @Override
  public boolean isUpdated() {
    return updated;
  }

  @Override
  public void setCached() {
    cached = true;
  }

}

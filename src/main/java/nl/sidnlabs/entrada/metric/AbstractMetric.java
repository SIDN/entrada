package nl.sidnlabs.entrada.metric;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class AbstractMetric implements Metric {

  protected String name;
  protected int value;
  protected long time;
  protected boolean cached;
  protected boolean updated;


  public AbstractMetric(String name, int value, long time) {
    this.name = name;
    this.value = value;
    this.time = time;
  }

  public void update(int value) {
    this.value += value;
    updated = true;
  }

  @Override
  public String toString() {
    return name + " " + value + " " + time;
  }

  @Override
  public int getSamples() {
    return -1;
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

package nl.sidnlabs.entrada.metric;

public interface Metric {

  void update(int value);

  /**
   * sample-size for calculated value
   * 
   * @return sample-size, -1 if value is based on single sample
   */
  int getSamples();

  boolean isCached();

  boolean isUpdated();

  long getTime();

  String getName();

  int getValue();

  void setCached();

}

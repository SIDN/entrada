package nl.sidnlabs.entrada.metric;

public interface Metric {

  String getName();

  int getValue();

  long getTime();

  void update(int value);

  /**
   * sample-size for calculated value
   * 
   * @return sample-size, -1 if value is based on single sample
   */
  int getSamples();

}

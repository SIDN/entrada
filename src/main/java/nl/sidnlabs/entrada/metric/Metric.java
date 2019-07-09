package nl.sidnlabs.entrada.metric;

public interface Metric {

  String getName();

  int getValue();

  long getTime();

  void update(int value);

}

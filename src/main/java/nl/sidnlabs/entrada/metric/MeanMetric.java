package nl.sidnlabs.entrada.metric;

import java.util.Arrays;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class MeanMetric implements Metric {

  private static final int MAX_SAMPLE_SIZE = 10000;
  private static final int INITIAL_SIZE = 1000;

  private String name;
  private long time;
  private int size;
  private int[] values = new int[INITIAL_SIZE];
  private int count;

  public MeanMetric(String name, int value, long time) {
    this.name = name;
    this.time = time;
    update(value);
  }

  public void update(int value) {
    if (size >= MAX_SAMPLE_SIZE) {
      // protection against filling up all the memory when lots
      // of tcp queries are received.
      return;
    }

    if (count == values.length - 1) {
      values = Arrays.copyOf(values, values.length + INITIAL_SIZE);
    }

    values[count] = value;
    count++;
  }


  public double median() {
    // cut off unused part of array
    values = Arrays.copyOf(values, count);
    // sort array
    Arrays.sort(values);
    double median;
    // check if total number of scores is even
    if (count % 2 == 0) {
      int sumOfMiddleElements = values[count / 2] + values[count / 2 - 1];
      // calculate average of middle elements
      median = ((double) sumOfMiddleElements) / 2;
    } else {
      // get the middle element
      median = (double) values[values.length / 2];
    }
    return median;
  }

  @Override
  public int getValue() {
    return (int) median();
  }

  @Override
  public int getSamples() {
    return count;
  }

}

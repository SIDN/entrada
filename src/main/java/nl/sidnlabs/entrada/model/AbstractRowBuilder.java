package nl.sidnlabs.entrada.model;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.model.Row.Column;

@Log4j2
public abstract class AbstractRowBuilder {

  protected static final int STATUS_COUNT = 100000;

  protected int packetCounter;
  private List<AddressEnrichment> enrichments;

  public AbstractRowBuilder(List<AddressEnrichment> enrichments) {
    this.enrichments = enrichments;
  }

  protected Column<String> column(String name, String value) {
    return new Column<>(name, value);
  }

  protected Column<Integer> column(String name, int value) {
    return new Column<>(name, Integer.valueOf(value));
  }

  protected Column<Boolean> column(String name, boolean value) {
    return new Column<>(name, Boolean.valueOf(value));
  }

  protected Column<Long> column(String name, long value) {
    return new Column<>(name, Long.valueOf(value));
  }

  protected void enrich(String address, String prefix, Row row) {

    String cleanPrefix = StringUtils.trimToEmpty(prefix);

    // execute all enrichments and if a match is found add value to row
    enrichments
        .stream()
        .filter(e -> e.match(address))
        .forEach(e -> row.addColumn(column(cleanPrefix + e.getColumn(), e.getValue())));
  }


  /**
   * replace all non printable ascii chars with the hex value of the char.
   * 
   * @param str string to filter
   * @return filtered version of input string
   */
  protected String filter(String str) {
    StringBuilder filtered = new StringBuilder(str.length());
    for (int i = 0; i < str.length(); i++) {
      char current = str.charAt(i);
      if (current >= 0x20 && current <= 0x7e) {
        filtered.append(current);
      } else {
        filtered.append("0x" + Integer.toHexString(current));
      }
    }

    return filtered.toString();
  }

  protected void showStatus() {
    log.info(packetCounter + " packets written to parquet file.");
  }

  protected void updateMetricMap(Map<Integer, Integer> map, Integer key) {
    Integer currentVal = map.get(key);
    if (currentVal != null) {
      map.put(key, currentVal.intValue() + 1);
    } else {
      map.put(key, 1);
    }
  }
}

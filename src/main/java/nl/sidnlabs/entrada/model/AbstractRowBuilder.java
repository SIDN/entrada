package nl.sidnlabs.entrada.model;

import java.net.InetAddress;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.cache2k.Cache;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.dnslib.util.Domaininfo;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.model.Row.Column;

@Log4j2
public abstract class AbstractRowBuilder implements RowBuilder {

  protected static final int STATUS_COUNT = 100000;

  protected long packetCounter;
  private List<AddressEnrichment> enrichments;
  protected HistoricalMetricManager metricManager;

  protected Cache<String, Domaininfo> cache;
  protected int domaininfoCacheHits;

  public AbstractRowBuilder(List<AddressEnrichment> enrichments,
      HistoricalMetricManager metricManager) {
    this.enrichments = enrichments;
    this.metricManager = metricManager;
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

  /**
   * Enrich row based on IP address, use both String and InetAddress params tp prevent having to
   * convert between the 2 too many times
   * 
   * @param address
   * @param inetAddress
   * @param prefix
   * @param row
   */
  protected void enrich(String address, InetAddress inetAddress, String prefix, Row row) {

    for (AddressEnrichment e : enrichments) {
      if (e.match(address, inetAddress)) {
        addColumn(row, prefix, e);
      }
    }
  }

  private void addColumn(Row row, String prefix, AddressEnrichment e) {
    row.addColumn(column(prefix + e.getColumn(), e.getValue()));

    if (StringUtils.equals(e.getColumn(), "country")) {
      metricManager
          .record(HistoricalMetricManager.METRIC_IMPORT_COUNTRY_COUNT + "." + e.getValue(), 1,
              row.getTs().getTime());
    }
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
    log.info(packetCounter + " rows written to parquet file.");
    log.info(domaininfoCacheHits + " domaininfo cache hits.");

  }

  @Override
  public void reset() {
    packetCounter = 0;
  }
}

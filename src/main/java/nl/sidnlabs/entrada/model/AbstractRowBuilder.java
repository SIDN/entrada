package nl.sidnlabs.entrada.model;

import java.net.InetAddress;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.springframework.beans.factory.annotation.Value;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.enrich.resolver.ResolverEnrichment;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.metric.Metric;
import nl.sidnlabs.entrada.model.Row.Column;

@Log4j2
public abstract class AbstractRowBuilder implements RowBuilder {

  protected static final int STATUS_COUNT = 100000;
  private final static int CACHE_MAX_SIZE = 50000;

  @Value("${entrada.privacy.enabled:false}")
  protected boolean privacy;
  @Value("${management.metrics.export.graphite.enabled:true}")
  protected boolean metricsEnabled;


  protected long packetCounter;
  private List<AddressEnrichment> enrichments;
  protected ServerContext serverCtx;

  protected int domainCacheHits;

  protected Cache<String, String> domainCache;

  public AbstractRowBuilder(List<AddressEnrichment> enrichments, ServerContext serverCtx) {
    this.enrichments = enrichments;
    this.serverCtx = serverCtx;

    domainCache = new Cache2kBuilder<String, String>() {}.entryCapacity(CACHE_MAX_SIZE).build();
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
  protected void enrich(String address, InetAddress inetAddress, String prefix, Row row,
      boolean skipResolvers, List<Metric> metrics) {

    // only perform checks that are required
    for (AddressEnrichment e : enrichments) {
      if (skipResolvers && e instanceof ResolverEnrichment) {
        continue;
      }

      String value = e.match(address, inetAddress);
      if (value != null) {
        addColumn(row, prefix, e.getColumn(), value, metrics);
      }
    }
  }

  private void addColumn(Row row, String prefix, String col, String value, List<Metric> metrics) {
    row.addColumn(column(prefix + col, value));

    if (metricsEnabled && StringUtils.equals(col, "country")) {

      metrics
          .add(HistoricalMetricManager
              .createMetric(HistoricalMetricManager.METRIC_IMPORT_COUNTRY_COUNT + "." + value, 1,
                  row.getTime(), true));

    }
  }

  protected void showStatus() {
    log.info(packetCounter + " rows created.");
    log.info(domainCacheHits + " domainname cache hits.");

  }

  @Override
  public void reset() {
    packetCounter = 0;
  }
}

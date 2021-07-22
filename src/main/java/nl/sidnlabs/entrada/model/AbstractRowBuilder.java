package nl.sidnlabs.entrada.model;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.enrich.resolver.ResolverEnrichment;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
public abstract class AbstractRowBuilder implements RowBuilder {

  protected static final int STATUS_COUNT = 100000;
  private final static int CACHE_MAX_SIZE = 50000;
  private final static int ENRICHMENT_CACHE_MAX_SIZE = 50000;

  @Value("${entrada.privacy.enabled:false}")
  protected boolean privacy;
  @Value("${management.metrics.export.graphite.enabled:true}")
  protected boolean metricsEnabled;

  protected Schema avroSchema;
  protected int counter;
  private List<AddressEnrichment> enrichments;
  protected ServerContext serverCtx;

  protected int domainCacheHits;
  protected int domainCacheInserted;

  protected Cache<String, String> domainCache;

  protected Cache<String, List<EnrichmentValue>> enrichmentCache;
  protected int cacheHits;
  protected int cacheInserts;


  public class EnrichmentValue {

    public EnrichmentValue(String name, String value, boolean resolver) {
      this.name = name;
      this.value = value;
      this.resolver = resolver;
    }

    public String name;
    public String value;
    public boolean resolver;
  }

  public AbstractRowBuilder(List<AddressEnrichment> enrichments, ServerContext serverCtx) {
    this.enrichments = enrichments;
    this.serverCtx = serverCtx;

    domainCache = new Cache2kBuilder<String, String>() {}.entryCapacity(CACHE_MAX_SIZE).build();
    enrichmentCache = new Cache2kBuilder<String, List<EnrichmentValue>>() {}
        .entryCapacity(ENRICHMENT_CACHE_MAX_SIZE)
        .build();
  }

  public Schema schema(String schema) {
    if (avroSchema != null) {
      // use cached version of schema
      return avroSchema;
    }

    try {
      Parser parser = new Schema.Parser().setValidate(true);
      avroSchema = parser.parse(new ClassPathResource(schema, getClass()).getInputStream());
    } catch (IOException e) {
      throw new ApplicationException("Cannot load schema from file: " + schema, e);
    }

    return avroSchema;
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
  protected boolean enrich(String address, InetAddress inetAddress, String prefix,
      GenericRecord record, boolean skipResolvers) {

    List<EnrichmentValue> cached = enrichmentCache.peek(address);
    if (cached != null) {
      for (EnrichmentValue ev : cached) {
        if (skipResolvers && ev.resolver) {
          continue;
        }

        // addColumn(record, prefix, ev.name, ev.value, metrics, time);
        record.put(prefix + ev.name, ev.value);
      }

      return true;
    }

    // not cached, do lookups and cache results

    cached = !skipResolvers ? new ArrayList<>() : null;
    // only perform checks that are required
    for (AddressEnrichment e : enrichments) {
      if (skipResolvers && e instanceof ResolverEnrichment) {
        continue;
      }

      String value = e.match(address, inetAddress);
      if (value != null) {
        // addColumn(record, prefix, e.getColumn(), value, metrics, time);

        record.put(prefix + e.getColumn(), value);

        if (cached != null) {
          cached.add(new EnrichmentValue(e.getColumn(), value, e instanceof ResolverEnrichment));
        }
      }
    }

    if (cached != null) {
      cacheInserts++;
      enrichmentCache.put(address, cached);
    }

    return false;
  }

  protected void showStatus() {
    log.info(counter + " rows created.");
    log.info(domainCacheHits + " domainname cache hits.");

  }

  @Override
  public void reset() {
    counter = 0;
    cacheHits = 0;
    cacheInserts = 0;
    domainCacheInserted = 0;
    domainCacheHits = 0;
  }

  public void printStats() {
    log.info("------------------ " + type().name() + " Row Builder stats ---------------------");
    log.info("Rows: {}", counter);
    log.info("Cache inserts (enrichment): {}", cacheInserts);
    log.info("Cache inserts (enrichment): {}%", percent(cacheInserts, counter));

    log.info("Cache hits (enrichment): {}", cacheHits);
    log.info("Cache hits (enrichment): {}%", percent(cacheHits, counter));

    log.info("Cache inserts (domain): {}", domainCacheInserted);
    log.info("Cache inserts (domain): {}%", percent(domainCacheInserted, counter));

    log.info("Cache hits (domain): {}", domainCacheHits);
    log.info("Cache hits (domain): {}%", percent(domainCacheHits, counter));
  }

  private String percent(int frac, int total) {
    if (total == 0) {
      return "0";
    }

    return String.format("%.2f", ((double) frac / total) * 100.0);

  }

}

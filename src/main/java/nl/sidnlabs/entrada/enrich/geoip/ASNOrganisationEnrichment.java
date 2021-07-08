package nl.sidnlabs.entrada.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.springframework.stereotype.Component;
import com.maxmind.geoip2.model.AsnResponse;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
// @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ASNOrganisationEnrichment implements AddressEnrichment {

  private final static int CACHE_MAX_SIZE = 25000;

  private Cache<String, String> cache;
  private GeoIPService geoLookup;

  public ASNOrganisationEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;

    cache = new Cache2kBuilder<String, String>() {}.entryCapacity(CACHE_MAX_SIZE).build();
  }

  // @Override
  // public String getValue() {
  // return value;
  // }

  /**
   * Lookup ASN for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with ASN if found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {
    String value = cache.peek(address);
    if (value != null) {
      return value;
    }

    Optional<? extends AsnResponse> r = geoLookup.lookupASN(inetAddress);
    if (r.isPresent()) {
      value = r.get().getAutonomousSystemOrganization();
      if (value != null) {
        cache.put(address, value);
        return value;
      }
    }

    return null;
  }

  @Override
  public String getColumn() {
    return "asn_organisation";
  }


}

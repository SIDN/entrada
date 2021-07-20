package nl.sidnlabs.entrada.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import org.springframework.stereotype.Component;
import com.maxmind.geoip2.model.CountryResponse;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
// @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CountryEnrichment implements AddressEnrichment {

  // private final static int CACHE_MAX_SIZE = 25000;

  // private Cache<String, String> cache;
  private GeoIPService geoLookup;

  public CountryEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;

    // cache = new Cache2kBuilder<String, String>() {}.entryCapacity(CACHE_MAX_SIZE).build();
  }

  // @Override
  // public String getValue() {
  // return value;
  // }

  /**
   * Lookup country for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with country if found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {
    // String value = cache.peek(address);
    // if (value != null) {
    // return value;
    // }

    Optional<CountryResponse> r = geoLookup.lookupCountry(inetAddress);
    if (r.isPresent()) {
      return r.get().getCountry().getIsoCode();
      // if (value != null) {
      // cache.put(address, value);
      // return value;
      // }
    }

    return null;
  }


  @Override
  public String getColumn() {
    return "country";
  }


}

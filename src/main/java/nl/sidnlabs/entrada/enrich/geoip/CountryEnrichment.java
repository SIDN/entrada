package nl.sidnlabs.entrada.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.springframework.stereotype.Component;
import com.maxmind.geoip2.model.CountryResponse;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
public class CountryEnrichment implements AddressEnrichment {

  private final static int CACHE_MAX_SIZE = 25000;

  private Cache<String, String> cache;
  private GeoIPService geoLookup;
  private String value;



  public CountryEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;

    cache = new Cache2kBuilder<String, String>() {}
        .name("geo-country-cache")
        .entryCapacity(CACHE_MAX_SIZE)
        .build();
  }

  @Override
  public String getValue() {
    return value;
  }

  /**
   * Lookup country for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with country if found
   */
  @Override
  public boolean match(InetAddress address) {
    String addr = address.getHostAddress();
    value = cache.peek(addr);
    if (value != null) {
      return true;
    }

    Optional<CountryResponse> r = geoLookup.lookupCountry(address);
    if (r.isPresent()) {
      value = r.get().getCountry().getIsoCode();
      if (value != null) {
        cache.put(addr, value);
        return true;
      }
    }

    return false;
  }


  @Override
  public String getColumn() {
    return "country";
  }


}

package nl.sidnlabs.entrada.enrich.geoip;

import java.util.Optional;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
public class CountryEnrichment implements AddressEnrichment {

  private GeoIPService geoLookup;
  private String value;

  public CountryEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;
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
  public boolean match(String address) {
    Optional<String> r = geoLookup.lookupCountry(address);
    if (r.isPresent()) {
      value = r.get();
      return true;
    }

    return false;
  }


  @Override
  public String getColumn() {
    return "country";
  }


}

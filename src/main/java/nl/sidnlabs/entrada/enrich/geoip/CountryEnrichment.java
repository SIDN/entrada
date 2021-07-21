package nl.sidnlabs.entrada.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import org.springframework.stereotype.Component;
import com.maxmind.geoip2.model.CountryResponse;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
public class CountryEnrichment implements AddressEnrichment {

  private GeoIPService geoLookup;

  public CountryEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;
  }


  /**
   * Lookup country for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with country if found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {

    Optional<CountryResponse> r = geoLookup.lookupCountry(inetAddress);
    if (r.isPresent()) {
      return r.get().getCountry().getIsoCode();
    }

    return null;
  }


  @Override
  public String getColumn() {
    return "country";
  }


}

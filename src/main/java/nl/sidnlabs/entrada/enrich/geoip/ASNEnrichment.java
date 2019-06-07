package nl.sidnlabs.entrada.enrich.geoip;

import java.util.Optional;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.ip.geo.GeoIPService;

@Component
public class ASNEnrichment implements AddressEnrichment {

  private GeoIPService geoLookup;
  private String value;

  public ASNEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;
  }

  @Override
  public String getValue() {
    return value;
  }

  /**
   * Lookup ASN for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with ASN if found
   */
  @Override
  public boolean match(String address) {
    Optional<String> r = geoLookup.lookupASN(address);
    if (r.isPresent()) {
      value = r.get();
      return true;
    }

    return false;
  }

  @Override
  public String getColumn() {
    return "asn";
  }


}

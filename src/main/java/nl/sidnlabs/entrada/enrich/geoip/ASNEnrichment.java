package nl.sidnlabs.entrada.enrich.geoip;

import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

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
    Optional<Pair<Integer, String>> r = geoLookup.lookupASN(address);
    if (r.isPresent()) {
      value = r.get().getKey() != null ? r.get().getKey() + "" : null;
      return true;
    }

    return false;
  }

  @Override
  public String getColumn() {
    return "asn";
  }


}

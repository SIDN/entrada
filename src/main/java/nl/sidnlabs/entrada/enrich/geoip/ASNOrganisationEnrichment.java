package nl.sidnlabs.entrada.enrich.geoip;

import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class ASNOrganisationEnrichment implements AddressEnrichment {

  private GeoIPService geoLookup;
  private String value;

  public ASNOrganisationEnrichment(GeoIPService geoLookup) {
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
      value = r.get().getValue() != null ? r.get().getValue() + "" : null;
      return true;
    }

    return false;
  }

  @Override
  public String getColumn() {
    return "asn_organisation";
  }


}

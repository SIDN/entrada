package nl.sidnlabs.entrada.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import org.springframework.stereotype.Component;
import com.maxmind.geoip2.model.AsnResponse;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
public class ASNEnrichment implements AddressEnrichment {

  private GeoIPService geoLookup;
  // private String value;

  public ASNEnrichment(GeoIPService geoLookup) {
    this.geoLookup = geoLookup;
  }


  /**
   * Lookup ASN for IP address
   * 
   * @param address IP address to perform lookup with
   * @return Optional with ASN if found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {

    Optional<? extends AsnResponse> r = geoLookup.lookupASN(inetAddress);
    if (r.isPresent()) {
      if (r.get().getAutonomousSystemNumber() != null) {
        return r.get().getAutonomousSystemNumber().toString();
      }
    }

    return null;
  }

  @Override
  public String getColumn() {
    return "asn";
  }


}

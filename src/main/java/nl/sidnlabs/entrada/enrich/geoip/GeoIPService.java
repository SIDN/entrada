package nl.sidnlabs.entrada.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CountryResponse;


public interface GeoIPService {

  void initialize();

  Optional<CountryResponse> lookupCountry(InetAddress addr);

  Optional<? extends AsnResponse> lookupASN(InetAddress ip);

}

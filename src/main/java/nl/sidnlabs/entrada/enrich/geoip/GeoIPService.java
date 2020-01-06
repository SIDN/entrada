package nl.sidnlabs.entrada.enrich.geoip;

import java.net.InetAddress;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;


public interface GeoIPService {

  void initialize();

  Optional<String> lookupCountry(String ip);

  Optional<String> lookupCountry(InetAddress addr);

  Optional<Pair<Integer, String>> lookupASN(InetAddress ip);

  Optional<Pair<Integer, String>> lookupASN(String ip);

}

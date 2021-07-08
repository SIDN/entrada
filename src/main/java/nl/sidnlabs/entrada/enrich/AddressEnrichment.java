package nl.sidnlabs.entrada.enrich;

import java.net.InetAddress;

public interface AddressEnrichment {

  String match(String address, InetAddress inetAddress);

  // String getValue();

  String getColumn();

}

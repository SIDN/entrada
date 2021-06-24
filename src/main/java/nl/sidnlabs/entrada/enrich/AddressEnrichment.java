package nl.sidnlabs.entrada.enrich;

import java.net.InetAddress;

public interface AddressEnrichment {

  boolean match(String address, InetAddress inetAddress);

  String getValue();

  String getColumn();

}

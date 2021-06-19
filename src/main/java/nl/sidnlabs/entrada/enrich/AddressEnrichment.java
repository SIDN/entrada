package nl.sidnlabs.entrada.enrich;

import java.net.InetAddress;

public interface AddressEnrichment {

  boolean match(InetAddress address);

  String getValue();

  String getColumn();

}

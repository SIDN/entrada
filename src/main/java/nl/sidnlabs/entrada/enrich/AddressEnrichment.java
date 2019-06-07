package nl.sidnlabs.entrada.enrich;

public interface AddressEnrichment {

  boolean match(String address);

  String getValue();

  String getColumn();

}

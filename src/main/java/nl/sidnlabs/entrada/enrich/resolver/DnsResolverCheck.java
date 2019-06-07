package nl.sidnlabs.entrada.enrich.resolver;

public interface DnsResolverCheck {

  void init();

  String getName();

  boolean match(String address);

  int getSize();

}

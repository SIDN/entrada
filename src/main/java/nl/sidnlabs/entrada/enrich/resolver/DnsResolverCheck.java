package nl.sidnlabs.entrada.enrich.resolver;

import java.net.InetAddress;

public interface DnsResolverCheck {

  void init();

  String getName();

  boolean match(String address, InetAddress inetAddress);

  int getMatcherCount();

  void done();
}

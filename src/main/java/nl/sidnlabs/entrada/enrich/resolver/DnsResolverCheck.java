package nl.sidnlabs.entrada.enrich.resolver;

import java.net.InetAddress;

public interface DnsResolverCheck {

  void init();

  String getName();

  boolean match(InetAddress address);

  int getMatchers();

  void done();
}

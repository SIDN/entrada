package nl.sidnlabs.entrada.enrich.resolver;

import java.net.InetAddress;
import java.util.List;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
public class ResolverEnrichment implements AddressEnrichment {

  private List<DnsResolverCheck> resolverChecks;
  private String value;

  public ResolverEnrichment(List<DnsResolverCheck> resolverChecks) {
    this.resolverChecks = resolverChecks;
  }

  @Override
  public String getValue() {
    return value;
  }

  /**
   * Check if the IP address is linked to a known open resolver operator
   * 
   * @param address IP address to perform lookup with
   * @return Optional with name of resolver operator, empty if no match found
   */
  @Override
  public boolean match(String address, InetAddress inetAddress) {

    for (DnsResolverCheck check : resolverChecks) {
      if (check.match(address, inetAddress)) {
        value = check.getName();
        return true;
      }
    }

    return false;
  }


  @Override
  public String getColumn() {
    return "pub_resolver";
  }


}

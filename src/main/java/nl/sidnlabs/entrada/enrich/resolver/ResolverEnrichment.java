package nl.sidnlabs.entrada.enrich.resolver;

import java.net.InetAddress;
import java.util.List;
import org.springframework.stereotype.Component;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;

@Component
// @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ResolverEnrichment implements AddressEnrichment {

  private List<DnsResolverCheck> resolverChecks;

  public ResolverEnrichment(List<DnsResolverCheck> resolverChecks) {
    this.resolverChecks = resolverChecks;
    for (DnsResolverCheck check : resolverChecks) {
      check.init();
    }
  }

  /**
   * Check if the IP address is linked to a known open resolver operator
   * 
   * @param address IP address to perform lookup with
   * @return Optional with name of resolver operator, empty if no match found
   */
  @Override
  public String match(String address, InetAddress inetAddress) {

    for (DnsResolverCheck check : resolverChecks) {
      if (check.match(address, inetAddress)) {
        String value = check.getName();
        return value;
      }
    }

    return null;
  }


  @Override
  public String getColumn() {
    return "pub_resolver";
  }


}

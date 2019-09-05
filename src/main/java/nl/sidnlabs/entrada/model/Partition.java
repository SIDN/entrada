package nl.sidnlabs.entrada.model;

import org.apache.commons.lang3.StringUtils;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Partition {

  private boolean dns;
  private int year;
  private int month;
  private int day;
  private String server;

  public String toPath() {
    if (dns) {
      return "year=" + year + "/month=" + month + "/day=" + day + "/server="
          + StringUtils.defaultIfBlank(StringUtils.deleteWhitespace(server), "__default__");
    }

    // icmp is partitioned by year/month/day only because there is not
    // that much data and adding "server" add partition key would create
    // even smaller data files
    return "year=" + year + "/month=" + month + "/day=" + day;
  }

}

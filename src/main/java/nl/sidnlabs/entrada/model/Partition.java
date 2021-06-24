package nl.sidnlabs.entrada.model;

import org.apache.commons.lang3.StringUtils;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Partition {

  private boolean dns;
  private int year;
  private int month;
  private int day;
  private String server;

  private String path;

  public String toPath() {
    if (path != null) {
      return path;
    }

    if (dns) {
      path = new StringBuilder()
          .append("year=")
          .append(year)
          .append("/month=")
          .append(month)
          .append("/day=")
          .append(day)
          .append("/server=")
          .append(StringUtils.defaultIfBlank(StringUtils.deleteWhitespace(server), "__default__"))
          .toString();
    } else {

      // icmp is partitioned by year/month/day only because there is not
      // that much data and adding "server" add partition key would create
      // even smaller data files

      path = new StringBuilder()
          .append("year=")
          .append(year)
          .append("/month=")
          .append(month)
          .append("/day=")
          .append(day)
          .toString();
    }

    return path;
  }

}

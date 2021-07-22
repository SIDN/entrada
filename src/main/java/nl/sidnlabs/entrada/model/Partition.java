package nl.sidnlabs.entrada.model;

import org.apache.commons.lang3.StringUtils;
import lombok.Builder;
import lombok.Getter;

@Getter
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

  @Override
  public String toString() {
    return toPath();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + day;
    result = prime * result + (dns ? 1231 : 1237);
    result = prime * result + month;
    result = prime * result + ((server == null) ? 0 : server.hashCode());
    result = prime * result + year;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Partition other = (Partition) obj;
    if (day != other.day)
      return false;
    if (dns != other.dns)
      return false;
    if (month != other.month)
      return false;
    if (server == null) {
      if (other.server != null)
        return false;
    } else if (!server.equals(other.server))
      return false;
    if (year != other.year)
      return false;
    return true;
  }

}

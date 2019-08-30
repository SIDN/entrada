package nl.sidnlabs.entrada.model;

import org.apache.commons.lang3.StringUtils;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Partition {

  private int year;
  private int month;
  private int day;
  private String server;

  public String toPath() {
    return "year=" + year + "/month=" + month + "/day=" + day + "/server="
        + StringUtils.defaultIfBlank(StringUtils.deleteWhitespace(server), "__default__");
  }



}

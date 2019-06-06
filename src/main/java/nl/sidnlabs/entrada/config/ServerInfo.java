package nl.sidnlabs.entrada.config;

import org.apache.commons.lang3.StringUtils;
import lombok.Data;

@Data
public class ServerInfo {
  private String fullname;
  private String name;
  private String location;

  public boolean hasAnycastLocation() {
    return StringUtils.isNotBlank(location);
  }

}

package nl.sidn.pcap.support;

import org.apache.commons.lang3.StringUtils;

public class ServerInfo {
  private String fullname;
  private String name;
  private String location;

  public String getFullname() {
    return fullname;
  }

  public void setFullname(String fullname) {
    this.fullname = fullname;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public boolean hasAnycastLocation() {
    return StringUtils.isNotBlank(location);
  }

}
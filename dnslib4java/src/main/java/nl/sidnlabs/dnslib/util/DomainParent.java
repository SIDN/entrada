package nl.sidnlabs.dnslib.util;

import lombok.Value;

@Value
public class DomainParent {
  private String match;
  private String parent;
  private int labels;
}

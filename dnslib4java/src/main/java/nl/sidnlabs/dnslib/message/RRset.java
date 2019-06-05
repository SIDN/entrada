/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with ENTRADA. If not, see
 * [<http://www.gnu.org/licenses/].
 *
 */
package nl.sidnlabs.dnslib.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import lombok.Data;
import nl.sidnlabs.dnslib.message.records.ResourceRecord;
import nl.sidnlabs.dnslib.types.ResourceRecordClass;
import nl.sidnlabs.dnslib.types.ResourceRecordType;

@Data
public class RRset implements Serializable {

  private static final long serialVersionUID = -8053869237612837919L;

  private List<ResourceRecord> data = new ArrayList<>();
  private String owner;
  private ResourceRecordClass classz;
  private ResourceRecordType type;

  public RRset() {}

  public RRset(String owner, ResourceRecordClass classz, ResourceRecordType type) {
    this.owner = owner;
    this.classz = classz;
    this.type = type;
  }

  public static RRset createAs(ResourceRecord rr) {
    RRset rrset = new RRset(rr.getName(), rr.getClassz(), rr.getType());
    rrset.add(rr);
    return rrset;
  }

  public void add(ResourceRecord rr) {
    if (rr.getName() == null) {
      throw new IllegalArgumentException("Trying to add an Invalid rr to the rrset: " + rr);
    }
    if (rr.getClassz() == classz && rr.getType() == type && rr.getName().equalsIgnoreCase(owner)) {
      data.add(rr);
    } else {
      throw new IllegalArgumentException("Trying to add an Invalid rr to the rrset: " + rr);
    }
  }

  public void remove(ResourceRecord rr) {
    if (rr.getClassz() == classz && rr.getType() == type && rr.getName().equalsIgnoreCase(owner)) {
      data.remove(rr);
    } else {
      throw new IllegalArgumentException("Trying to remove an Invalid rr from the rrset: " + rr);
    }
  }

  public void clear() {
    data.clear();
  }

  public List<ResourceRecord> getAll() {
    return data;
  }

  public int size() {
    return data.size();
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("RRset\n [ owner=" + owner + ", classz=" + classz + ", type=" + type + "\n");

    for (ResourceRecord rr : data) {
      b.append(rr.toString());
      b.append("\n");
    }

    b.append(" ]");

    return b.toString();
  }

  public Object toZone(int maxLength) {
    StringBuilder b = new StringBuilder();

    for (ResourceRecord rr : data) {
      b.append(rr.toZone(maxLength));
      b.append("\n");
    }

    return b.toString();
  }

  public JsonArray toJSon() {
    JsonArrayBuilder builder = Json.createArrayBuilder();

    for (ResourceRecord rr : data) {
      builder.add(rr.toJSon());
    }

    return builder.build();
  }

}

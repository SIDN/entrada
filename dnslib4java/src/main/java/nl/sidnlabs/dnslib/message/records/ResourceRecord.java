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
package nl.sidnlabs.dnslib.message.records;

import javax.json.JsonObject;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.ResourceRecordClass;
import nl.sidnlabs.dnslib.types.ResourceRecordType;

public interface ResourceRecord {

  String getName();

  void setName(String name);

  ResourceRecordType getType();

  void setType(ResourceRecordType type);

  ResourceRecordClass getClassz();

  void setClassz(ResourceRecordClass classz);

  long getTtl();

  void setTtl(long ttl);

  char getRdLength();

  byte[] getRdata();

  void decode(NetworkData buffer);

  void encode(NetworkData buffer);

  String toZone(int maxLength);

  JsonObject toJSon();

}

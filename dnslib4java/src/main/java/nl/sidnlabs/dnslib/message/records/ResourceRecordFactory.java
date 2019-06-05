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

import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.dnslib.message.records.dnssec.DNSKEYResourceRecord;
import nl.sidnlabs.dnslib.message.records.dnssec.DSResourceRecord;
import nl.sidnlabs.dnslib.message.records.dnssec.NSEC3ParamResourceRecord;
import nl.sidnlabs.dnslib.message.records.dnssec.NSEC3ResourceRecord;
import nl.sidnlabs.dnslib.message.records.dnssec.NSECResourceRecord;
import nl.sidnlabs.dnslib.message.records.dnssec.RRSIGResourceRecord;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;

@Log4j2
public class ResourceRecordFactory {

  /*
   * TYPE value and meaning -------------------------------------------------------- A 1 a host
   * address NS 2 an authoritative name server MD 3 a mail destination (Obsolete - use MX) MF 4 a
   * mail forwarder (Obsolete - use MX) CNAME 5 the canonical name for an alias SOA 6 marks the
   * start of a zone of authority MB 7 a mailbox domain name (EXPERIMENTAL) MG 8 a mail group member
   * (EXPERIMENTAL) MR 9 a mail rename domain name (EXPERIMENTAL) NULL 10 a null RR (EXPERIMENTAL)
   * WKS 11 a well known service description PTR 12 a domain name pointer HINFO 13 host information
   * MINFO 14 mailbox or mail list information MX 15 mail exchange TXT 16 text strings
   * 
   * AAAA 28 ipv6 host address DNSKEY 48 RRSIG 46 NSEC 47 DS 43 NSEC3 50
   */

  private static ResourceRecordFactory instance = new ResourceRecordFactory();

  private ResourceRecordFactory() {};

  public static ResourceRecordFactory getInstance() {
    return instance;
  }

  public ResourceRecord createResourceRecord(int type) {

    switch (type) {
      case 1:
        return new AResourceRecord();
      case 2:
        return new NSResourceRecord();
      case 28:
        return new AAAAResourceRecord();
      case 5:
        return new CNAMEResourceRecord();
      case 6:
        return new SOAResourceRecord();
      case 12:
        return new PTRResourceRecord();
      case 13:
        return new HINFOResourceRecord();
      case 15:
        return new MXResourceRecord();
      case 16:
        return new TXTResourceRecord();
      case 29:
        return new LOCResourceRecord();
      case 33:
        return new SRVResourceRecord();
      case 35:
        return new NAPTRResourceRecord();
      case 41:
        return new OPTResourceRecord();
      case 43:
        return new DSResourceRecord();
      case 44:
        return new SSHFPResourceRecord();
      case 46:
        return new RRSIGResourceRecord();
      case 47:
        return new NSECResourceRecord();
      case 48:
        return new DNSKEYResourceRecord();
      case 50:
        return new NSEC3ResourceRecord();
      case 51:
        return new NSEC3ParamResourceRecord();
      case 99:
        return new SPFResourceRecord();
      case 255:
        return new AnyResourceRecord();
      case 256:
        return new URIResourceRecord();
    }

    if (log.isDebugEnabled()) {
      log.debug("Unknown RR with type " + type);
    }
    return new NotImplementedResourceRecord();
  }

}

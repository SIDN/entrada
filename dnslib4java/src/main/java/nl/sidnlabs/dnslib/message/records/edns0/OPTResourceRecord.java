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
package nl.sidnlabs.dnslib.message.records.edns0;

import java.util.ArrayList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.apache.log4j.Logger;
import lombok.Data;
import lombok.EqualsAndHashCode;
import nl.sidnlabs.dnslib.message.records.AbstractResourceRecord;
import nl.sidnlabs.dnslib.message.util.DNSStringUtil;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.ResourceRecordType;

/**
 * 
 * EDNS Header Flags (16 bits)
 * 
 * Registration Procedures Standards Action Reference [RFC-ietf-dnsext-rfc2671bis-edns0-10] Bit Flag
 * Description Reference Bit 0 DO DNSSEC answer OK [RFC4035][RFC3225] Bit 1-15 Reserved
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class OPTResourceRecord extends AbstractResourceRecord {

  private static final long serialVersionUID = 1L;

  private static final Logger LOGGER = Logger.getLogger(OPTResourceRecord.class);

  private static final char DNSSEC_DO_BIT_MASK = 0x8000; // 1000 0000 0000 0000

  private static final int POWERDNS_EDNSPING_UDPSIZE = 1200;
  private static final int POWERDNS_EDNSPING_LENGTH = 4;

  private char udpPlayloadSize = 4096;

  private char rdLeng = 0;

  private short rcode;
  private short version;
  private char flags;

  private boolean dnssecDo;

  private List<EDNS0Option> options = new ArrayList<>();

  public OPTResourceRecord() {
    name = ".";
    type = ResourceRecordType.OPT;
  }

  @Override
  public void decode(NetworkData buffer) {
    // name
    name = DNSStringUtil.readName(buffer);

    char type = buffer.readUnsignedChar();
    setType(ResourceRecordType.fromValue(type));

    udpPlayloadSize = buffer.readUnsignedChar();

    rcode = buffer.readUnsignedByte();

    version = buffer.readUnsignedByte();

    flags = buffer.readUnsignedChar();

    dnssecDo = (flags & DNSSEC_DO_BIT_MASK) == DNSSEC_DO_BIT_MASK;

    rdLeng = buffer.readUnsignedChar();
    if (rdLeng > 0) {
      // not tested yet, add try catch just to be safe
      try {
        loadOptions(buffer);
      } catch (Exception e) {
        // ignore
        LOGGER.error("Could not decode OPT RR", e);
      }
    }
  }

  private void loadOptions(NetworkData buffer) {
    if (rdLeng > buffer.bytesAvailable()) {
      LOGGER.error("Incorrect edns rdata size, rdlength=" + (int) rdLeng + " and bytesavail:"
          + buffer.bytesAvailable());
      return;
    }

    byte[] optionBytes = new byte[rdLeng];
    buffer.readBytes(optionBytes);

    NetworkData opt = new NetworkData(optionBytes);
    while (opt.isBytesAvailable()) {
      EDNS0Option option = decodeOption(opt);
      options.add(option);
    }
  }

  /**
   * Detect the code and len of the edns0 option and create the correct option object to return.
   * 
   * @param opt
   * @return edns0 option object
   */
  private EDNS0Option decodeOption(NetworkData opt) {
    int optioncode = opt.readUnsignedChar();
    int optionlen = opt.readUnsignedChar();

    if (optioncode == 3) { // nsid
      return new NSidOption(optioncode, optionlen, opt);
    } else if (optioncode == 5) {
      // optioncode 5 can be two things:
      // 1 dnssec DAU http://tools.ietf.org/html/rfc6975
      // 2 ednsping http://tools.ietf.org/html/draft-hubert-ulevitch-edns-ping-01

      // powerdns edns ping uses udp size 1200
      // option length 4
      if (udpPlayloadSize == POWERDNS_EDNSPING_UDPSIZE && optionlen == POWERDNS_EDNSPING_LENGTH) {
        return new PingOption(optioncode, optionlen, opt);
      } else {
        // must be dnssec DAU
        return new DNSSECOption(optioncode, optionlen, opt);
      }
    } else if (optioncode == 6 || optioncode == 7) {
      // decode dnssec option
      return new DNSSECOption(optioncode, optionlen, opt);
    } else if (optioncode == 8) {
      // decode clientsubnet option
      return new ClientSubnetOption(optioncode, optionlen, opt);
    } else if (optioncode == 12) { // Padding
      return new PaddingOption(optioncode, optionlen, opt);
    } else if (optioncode == 14) { // dns key
      return new KeyTagOption(optioncode, optionlen, opt);
    } else {
      return new EDNS0Option(optioncode, optionlen, opt);
    }
  }


  @Override
  public void encode(NetworkData buffer) {

    LOGGER.debug("encode");

    // write the name
    buffer.writeByte(0);

    // write the opt type
    buffer.writeChar(type.getValue());

    // write the supported udp size
    buffer.writeChar(udpPlayloadSize);

    // write extended rcode
    buffer.writeByte(0x0);

    // write version
    buffer.writeByte(0x0);

    // default all flags off
    char flags = 0x0;

    // dnssec enabled, signal with do bit is on
    flags = (char) (flags | DNSSEC_DO_BIT_MASK);

    // write all the flags
    buffer.writeChar(flags);

    // write the length of the rdata section
    buffer.writeChar(rdLeng);
  }


  @Override
  public JsonObject toJSon() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    return builder.add("name", name).add("type", type.name())
        .add("payload-size", (int) udpPlayloadSize).add("rcode", rcode).add("flags", (int) flags)
        .add("rdata", Json.createObjectBuilder().add("do", dnssecDo)).build();
  }

  @Override
  public String toZone(int maxLength) {
    return "";
  }

}

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

import javax.json.Json;
import javax.json.JsonObject;
import lombok.Data;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.dnslib.types.OpcodeType;
import nl.sidnlabs.dnslib.types.RcodeType;

@Data
public class Header {

  /*
   * 
   * 1 1 1 1 1 1 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ |
   * ID | +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ |QR| Opcode |AA|TC|RD|RA| Z | RCODE |
   * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ | QDCOUNT |
   * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ | ANCOUNT |
   * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ | NSCOUNT |
   * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ | ARCOUNT |
   * +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
   * 
   * where:
   * 
   * ID A 16 bit identifier assigned by the program that generates any kind of query. This
   * identifier is copied the corresponding reply and can be used by the requester to match up
   * replies to outstanding queries.
   * 
   * QR A one bit field that specifies whether this message is a query (0), or a response (1).
   * 
   * OPCODE A four bit field that specifies kind of query in this message. This value is set by the
   * originator of a query and copied into the response.
   * 
   * AA Authoritative Answer - this bit is valid in responses, and specifies that the responding
   * name server is an authority for the domain name in question section.
   * 
   * Note that the contents of the answer section may have multiple owner names because of aliases.
   * The AA bit corresponds to the name which matches the query name, or the first owner name in the
   * answer section.
   * 
   * TC TrunCation - specifies that this message was truncated due to length greater than that
   * permitted on the transmission channel.
   * 
   * RD Recursion Desired - this bit may be set in a query and is copied into the response. If RD is
   * set, it directs the name server to pursue the query recursively. Recursive query support is
   * optional.
   * 
   * RA Recursion Available - this be is set or cleared in a response, and denotes whether recursive
   * query support is available in the name server.
   * 
   * Z Reserved for future use. Must be zero in all queries and responses.
   * 
   * RCODE Response code - this 4 bit field is set as part of responses. The values have the
   * following
   * 
   * QDCOUNT an unsigned 16 bit integer specifying the number of entries in the question section.
   * 
   * ANCOUNT an unsigned 16 bit integer specifying the number of resource records in the answer
   * section.
   * 
   * NSCOUNT an unsigned 16 bit integer specifying the number of name server resource records in the
   * authority records section.
   * 
   * ARCOUNT an unsigned 16 bit integer specifying the number of resource records in the additional
   * records section.
   * 
   * 
   */

  private static final int QR_QUERY_BIT_MASK = 32768; // 1000 0000 0000 0000
  private static final int AA_BIT_MASK = 1024; // 0000 0100 0000 0000
  private static final int TC_BIT_MASK = 512; // 0000 0010 0000 0000
  private static final int RD_BIT_MASK = 256; // 0000 0001 0000 0000
  private static final int RA_BIT_MASK = 128; // 0000 0000 1000 0000
  private static final int Z_BIT_MASK = 64; // 0000 0000 0100 0000
  private static final int AD_BIT_MASK = 32; // 0000 0000 0010 0000
  private static final int CD_BIT_MASK = 16; // 0000 0000 0001 0000

  private static final int OPCODE_MASK = 30720; // 0111 1000 0000 0000
  private static final int RCODE_MASK = 15; // 0000 0000 0000 1111

  private int id;
  private MessageType qr = MessageType.QUERY;
  private OpcodeType opCode;
  private boolean aa;
  private boolean tc;
  private boolean rd;
  private boolean ra;
  private boolean z;
  private boolean ad;
  private boolean cd;
  private RcodeType rcode;
  private char qdCount;
  private char anCount;
  private char nsCount;
  private char arCount;

  private int rawOpcode;
  private int rawRcode;

  public void decode(NetworkData buffer) {
    // get the message id
    setId(buffer.readUnsignedChar());
    // decode the flags (next 16 bits)
    decodeFlags(buffer);

    // read the counters for the RR's
    setQdCount(buffer.readUnsignedChar());
    setAnCount(buffer.readUnsignedChar());
    setNsCount(buffer.readUnsignedChar());
    setArCount(buffer.readUnsignedChar());
  }

  private void decodeFlags(NetworkData buffer) {

    char flags = buffer.readUnsignedChar();

    if ((flags & QR_QUERY_BIT_MASK) == QR_QUERY_BIT_MASK) {
      // the message is a response
      setQr(MessageType.RESPONSE);
    } else {
      // the message is a query
      setQr(MessageType.QUERY);
    }

    /*
     * OPCODE A four bit field that specifies kind of query in this message. This value is set by
     * the originator of a query and copied into the response. The values are:
     * 
     * 0 a standard query (QUERY) 1 an inverse query (IQUERY) 2 a server status request (STATUS)
     * 3-15 reserved for future use
     * 
     */
    rawOpcode = (flags & OPCODE_MASK);
    // shift bits to end of 16bit char
    rawOpcode = rawOpcode >>> 11;
    if (rawOpcode == OpcodeType.STANDARD.getValue()) {
      setOpCode(OpcodeType.STANDARD);
    } else if (rawOpcode == OpcodeType.INVERSE.getValue()) {
      setOpCode(OpcodeType.INVERSE);
    } else if (rawOpcode == OpcodeType.STATUS.getValue()) {
      setOpCode(OpcodeType.STATUS);
    } else if (rawOpcode == OpcodeType.NOTIFY.getValue()) {
      setOpCode(OpcodeType.NOTIFY);
    } else if (rawOpcode == OpcodeType.UPPDATE.getValue()) {
      setOpCode(OpcodeType.UPPDATE);
    } else {
      setOpCode(OpcodeType.UNASSIGNED);
    }

    /*
     * AA Authoritative Answer - this bit is valid in responses, and specifies that the responding
     * name server is an authority for the domain name in question section.
     */

    setAa((flags & AA_BIT_MASK) == AA_BIT_MASK);

    /*
     * TC TrunCation - specifies that this message was truncated due to length greater than that
     * permitted on the transmission channel.
     */
    setTc((flags & TC_BIT_MASK) == TC_BIT_MASK);

    /*
     * RD Recursion Desired - this bit may be set in a query and is copied into the response. If RD
     * is set, it directs the name server to pursue the query recursively. Recursive query support
     * is optional.
     */
    setRd((flags & RD_BIT_MASK) == RD_BIT_MASK);

    /*
     * RA Recursion Available - this be is set or cleared in a response, and denotes whether
     * recursive query support is available in the name server.
     */
    setRa((flags & RA_BIT_MASK) == RA_BIT_MASK);

    /*
     * Z Reserved for future use. Must be zero in all queries and responses.
     */
    setZ((flags & Z_BIT_MASK) == Z_BIT_MASK);

    /*
     * DNSSEC allocates two new bits in the DNS message header: the CD (Checking Disabled) bit and
     * the AD (Authentic Data) bit. The CD bit is controlled by resolvers; a security-aware name
     * server MUST copy the CD bit from a query into the corresponding response. The AD bit is
     * controlled by name servers; a security-aware name server MUST ignore the setting of the AD
     * bit in queries. See Sections 3.1.6, 3.2.2, 3.2.3, 4, and 4.9 for details on the behavior of
     * these bits.
     */

    /* AD Authentic data (DNSSEC) */
    setAd((flags & AD_BIT_MASK) == AD_BIT_MASK);

    /* CD Checking Disabled - non-authenticated data is acceptable (DNSSEC) */
    setCd((flags & CD_BIT_MASK) == CD_BIT_MASK);

    /*
     * RCODE Response code - this 4 bit field is set as part of responses. The values have the
     * following interpretation:
     * 
     * 0 No error condition
     * 
     * 1 Format error - The name server was unable to interpret the query.
     * 
     * 2 Server failure - The name server was unable to process this query due to a problem with the
     * name server.
     * 
     * 3 Name Error - Meaningful only for responses from an authoritative name server, this code
     * signifies that the domain name referenced in the query does not exist.
     * 
     * 4 Not Implemented - The name server does not support the requested kind of query.
     * 
     * 5 Refused - The name server refuses to perform the specified operation for policy reasons.
     * For example, a name server may not wish to provide the information to the particular
     * requester, or a name server may not wish to perform a particular operation (e.g., zone
     * transfer) for particular data.
     * 
     * 6-15 Reserved for future use.
     */
    rawRcode = (flags & RCODE_MASK);
    setRcode(RcodeType.fromValue(rawRcode));
  }


  public void encode(NetworkData buffer) {

    // write unique 16bit id for the packet
    buffer.writeChar(getId());

    /*
     * create a bitmask for the header status flags. start with all flags to zero and flip the bits
     * where apropriate.
     * 
     * 0000 0000 0000 0000 16 bit mask
     */
    char flags = 0x0;

    if (qr == MessageType.RESPONSE) {
      // flip the response flag
      flags = (char) (flags | 0x8000); // 1000 0000 0000 0000
    }

    if (opCode == OpcodeType.INVERSE) {
      flags = (char) (flags | 0x800); // 0000 1000 0000 0000
    } else if (opCode == OpcodeType.STATUS) {
      flags = (char) (flags | 0x1000); // 0001 0000 0000 0000
    }

    if (aa) {
      flags = (char) (flags | 0x400); // 0000 0100 0000 0000
    }

    if (tc) {
      flags = (char) (flags | 0x200); // 0000 0010 0000 0000
    }

    if (rd) {
      flags = (char) (flags | 0x100); // 0000 0001 0000 0000
    }

    if (ra) {
      flags = (char) (flags | 0x80); // 0000 0000 1000 0000
    }

    if (z) {
      flags = (char) (flags | 0x40); // 0000 0000 0100 0000
    }

    if (ad) {
      flags = (char) (flags | 0x20); // 0000 0000 0010 0000
    }

    if (cd) {
      flags = (char) (flags | 0x10); // 0000 0000 0001 0000
    }
    // determine the correct rcode bitmask
    if (rcode == RcodeType.FORMAT_ERROR) {
      flags = (char) (flags | 0x1); // 0000 0000 0000 0001
    } else if (rcode == RcodeType.SERVER_FAILURE) {
      flags = (char) (flags | 0x2); // 0000 0000 0000 0010
    } else if (rcode == RcodeType.NXDOMAIN) {
      flags = (char) (flags | 0x3); // 0000 0000 0000 0011
    } else if (rcode == RcodeType.NOT_IMPLEMENTED) {
      flags = (char) (flags | 0x4); // 0000 0000 0000 0100
    } else if (rcode == RcodeType.REFUSED) {
      flags = (char) (flags | 0x5); // 0000 0000 0000 0101
    }

    // write the flags
    buffer.writeChar(flags);
    // question count
    buffer.writeChar((char) getQdCount());
    // an count
    buffer.writeChar((char) getAnCount());
    // ns count
    buffer.writeChar((char) getNsCount());
    // ar count
    buffer.writeChar((char) getArCount());

  }

  public static Header createResponseHeader(char id) {
    Header hdr = new Header();
    hdr.setId(id);
    hdr.setQr(MessageType.RESPONSE);
    return hdr;
  }

  public String toZone() {
    return "flags: aa:" + aa + ", tc:" + tc + ", rd:" + rd + ", ra:" + ra + ", ad:" + ad + ", cd:"
        + cd + ", rcode:" + rcode + ", qdCount:" + (int) qdCount + ", anCount:" + (int) anCount
        + ", nsCount:" + (int) nsCount + ", arCount:" + (int) arCount;
  }

  public JsonObject toJSon() {
    return Json
        .createObjectBuilder()
        .add("id", id)
        .add("qr", qr.getValue())
        .add("opcode", opCode.getValue())
        .add("flags",
            Json
                .createObjectBuilder()
                .add("aa", aa)
                .add("tc", tc)
                .add("rd", rd)
                .add("ra", ra)
                .add("ad", ad)
                .add("cd", cd))
        .add("rcode", rcode.name())
        .add("qdCount", qdCount)
        .add("anCount", anCount)
        .add("nsCount", nsCount)
        .add("arCount", arCount)
        .build();
  }

}

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

import java.util.ArrayList;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import lombok.Data;
import nl.sidnlabs.dnslib.message.records.NotImplementedResourceRecord;
import nl.sidnlabs.dnslib.message.records.ResourceRecord;
import nl.sidnlabs.dnslib.message.records.ResourceRecordFactory;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidnlabs.dnslib.message.util.DNSStringUtil;
import nl.sidnlabs.dnslib.message.util.NetworkData;
import nl.sidnlabs.dnslib.types.ResourceRecordType;

@Data
public class Message {

  // size of msg in bytes
  private int bytes;
  private Header header;

  private List<Question> questions = new ArrayList<>();
  private List<RRset> answer = new ArrayList<>();
  private List<RRset> authority = new ArrayList<>();
  private List<RRset> additional = new ArrayList<>();
  // convenience list for fast acces to unknown rrs
  private List<NotImplementedResourceRecord> unknownRRs = new ArrayList<>();

  private OPTResourceRecord pseudo;

  public Message() {};

  public Message(NetworkData data, boolean allowFail) {

    try {
      decode(data);
    } catch (Exception e) {
      if (!allowFail) {
        // rethrow exception
        throw e;
      }
      // if allowFail then ignore exceptions.
    }
  };

  public Message(NetworkData data) {
    this(data, false);
  };

  public Header getHeader() {
    return header;
  }

  public Message addHeader(Header header) {
    this.header = header;
    updateHeaderCounters();
    return this;
  }

  public Message build() {
    updateHeaderCounters();
    return this;
  }

  public void updateHeaderCounters() {
    this.header.setQdCount((char) questions.size());
    this.header.setAnCount((char) rrsetSize(answer));
    this.header.setNsCount((char) rrsetSize(authority));
    this.header.setArCount((char) rrsetSize(additional));
    // opt rr is part of additional section
    if (pseudo != null)
      this.header.setArCount((char) (this.header.getArCount() + 1));
  }

  private int rrsetSize(List<RRset> rrsets) {
    int count = 0;
    for (RRset rrset : rrsets) {
      count = count + rrset.size();
    }
    return count;
  }

  public Message addQuestion(Question question) {
    this.questions.add(question);
    return this;
  }

  private RRset findRRset(List<RRset> setList, ResourceRecord rr) {
    for (RRset rrset : setList) {
      if (rrset.getOwner().equalsIgnoreCase(rr.getName()) && rrset.getClassz() == rr.getClassz()
          && rrset.getType() == rr.getType()) {
        return rrset;
      }
    }

    return null;
  }

  private RRset createRRset(List<RRset> setList, ResourceRecord rr) {
    RRset rrset = RRset.createAs(rr);
    setList.add(rrset);
    return rrset;
  }

  public void addAnswer(ResourceRecord answer) {
    RRset rrset = findRRset(this.answer, answer);
    if (rrset == null) {
      rrset = createRRset(this.answer, answer);
    } else {
      rrset.add(answer);
    }
  }

  public void addAnswer(RRset rrset) {
    answer.add(rrset);
  }

  public void addAuthority(ResourceRecord authority) {
    RRset rrset = findRRset(this.authority, authority);
    if (rrset == null) {
      rrset = createRRset(this.authority, authority);
    } else {
      rrset.add(authority);
    }
  }

  public void addAuthority(RRset authority) {
    this.authority.add(authority);

  }

  public List<RRset> getAdditional() {
    return additional;
  }

  public void addAdditional(ResourceRecord rr) {
    RRset rrset = findRRset(this.additional, rr);
    if (rrset == null) {
      rrset = createRRset(this.additional, rr);
    } else {
      rrset.add(rr);
    }
  }

  public void addAdditional(RRset additional) {
    if (additional.getType() != ResourceRecordType.OPT) {
      this.additional.add(additional);
    }
  }

  public void decode(NetworkData buffer) {
    header = new Header();
    header.decode(buffer);

    // decode all questions in the message
    for (int i = 0; i < header.getQdCount(); i++) {
      Question question = decodeQuestion(buffer);
      addQuestion(question);
    }

    for (int i = 0; i < header.getAnCount(); i++) {
      ResourceRecord rr = decodeResourceRecord(buffer);
      addAnswer(rr);
    }

    for (int i = 0; i < header.getNsCount(); i++) {
      ResourceRecord rr = decodeResourceRecord(buffer);
      addAuthority(rr);
    }

    for (int i = 0; i < header.getArCount(); i++) {
      ResourceRecord rr = decodeResourceRecord(buffer);
      if (rr.getType() != ResourceRecordType.OPT) {
        addAdditional(rr);
      } else {
        pseudo = (OPTResourceRecord) (rr);
      }
    }

    /*
     * not all RR may have been decoded into the message to make sure the section counters are
     * correct do an update of the counters now.
     */
    updateHeaderCounters();
  }

  private ResourceRecord decodeResourceRecord(NetworkData buffer) {

    /*
     * read ahead to the type bytes to find out what type of RR needs to be created.
     */

    // skip 16bits with name
    buffer.markReaderIndex();
    // read the name to get to the type bytes after the name
    DNSStringUtil.readName(buffer);

    // read 16 bits with type
    int type = buffer.readUnsignedChar();

    // go back bits to the start of the RR
    buffer.resetReaderIndex();

    ResourceRecord rr = ResourceRecordFactory.getInstance().createResourceRecord(type);

    if (rr instanceof NotImplementedResourceRecord) {
      unknownRRs.add((NotImplementedResourceRecord) rr);
    }

    if (rr != null) {
      // decode the entire rr now
      rr.decode(buffer);
    }
    return rr;
  }


  private Question decodeQuestion(NetworkData buffer) {

    Question question = new Question();

    question.decode(buffer);

    return question;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("\nheader\n");
    builder.append("_______________________________________________\n");
    builder.append("Message [header=" + header + "] ");
    builder.append("\n");

    builder.append("question\n");
    builder.append("_______________________________________________\n");
    for (Question question : questions) {
      builder.append(question.toString());
      builder.append("\n");
    }

    builder.append("answer\n");
    builder.append("_______________________________________________\n");
    for (RRset rrset : answer) {
      builder.append(rrset.toString());
      builder.append("\n");
    }

    builder.append("authority\n");
    builder.append("_______________________________________________\n");
    for (RRset rrset : authority) {
      builder.append(rrset.toString());
      builder.append("\n");
    }

    builder.append("additional\n");
    builder.append("_______________________________________________\n");
    for (RRset rrset : additional) {
      builder.append(rrset.toString());
      builder.append("\n");
    }

    return builder.toString();
  }

  public Object toZone() {
    StringBuilder builder = new StringBuilder();
    builder.append("; header: " + header.toZone() + "\n");

    int maxLength = maxLength();
    builder.append("; answer section:\n");
    for (RRset rrset : answer) {
      builder.append(rrset.toZone(maxLength));
    }

    builder.append("; authority section:\n");
    for (RRset rrset : authority) {
      builder.append(rrset.toZone(maxLength));
    }

    builder.append("; additional section:\n");
    for (RRset rrset : additional) {
      builder.append(rrset.toZone(maxLength));
    }

    return builder.toString();
  }

  public JsonObject toJson() {
    JsonObjectBuilder builder = Json.createObjectBuilder();
    builder.add("header", header.toJSon());

    JsonArrayBuilder questionsBuilder = Json.createArrayBuilder();
    for (Question question : questions) {
      questionsBuilder.add(question.toJSon());
    }

    builder.add("question", questionsBuilder.build());

    JsonArrayBuilder rrBuilder = Json.createArrayBuilder();
    for (RRset rrset : answer) {
      rrBuilder.add(rrset.toJSon());
    }
    builder.add("answer", rrBuilder.build());

    rrBuilder = Json.createArrayBuilder();
    for (RRset rrset : authority) {
      rrBuilder.add(rrset.toJSon());
    }
    builder.add("authority", rrBuilder.build());

    rrBuilder = Json.createArrayBuilder();
    for (RRset rrset : additional) {
      rrBuilder.add(rrset.toJSon());
    }
    builder.add("additional", rrBuilder.build());

    return builder.build();
  }

  public int maxLength() {
    int length = 0;

    for (RRset rrset : answer) {
      if (rrset.getOwner().length() > length) {
        length = rrset.getOwner().length();
      }
    }

    for (RRset rrset : authority) {
      if (rrset.getOwner().length() > length) {
        length = rrset.getOwner().length();
      }
    }

    for (RRset rrset : additional) {
      if (rrset.getOwner().length() > length) {
        length = rrset.getOwner().length();
      }
    }

    return length;
  }


  public boolean isUnknownRRFound() {
    return !unknownRRs.isEmpty();
  }

  public List<NotImplementedResourceRecord> getUnknownRRs() {
    return unknownRRs;
  }

}

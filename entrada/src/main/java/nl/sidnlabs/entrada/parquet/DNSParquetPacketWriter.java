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
package nl.sidnlabs.entrada.parquet;

import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import com.google.common.base.Joiner;
import nl.sidn.dnslib.message.Header;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.Question;
import nl.sidn.dnslib.message.records.edns0.ClientSubnetOption;
import nl.sidn.dnslib.message.records.edns0.DNSSECOption;
import nl.sidn.dnslib.message.records.edns0.EDNS0Option;
import nl.sidn.dnslib.message.records.edns0.KeyTagOption;
import nl.sidn.dnslib.message.records.edns0.NSidOption;
import nl.sidn.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidn.dnslib.message.records.edns0.PaddingOption;
import nl.sidn.dnslib.message.records.edns0.PingOption;
import nl.sidn.dnslib.types.OpcodeType;
import nl.sidn.dnslib.types.RcodeType;
import nl.sidn.dnslib.types.ResourceRecordType;
import nl.sidn.dnslib.util.Domaininfo;
import nl.sidn.dnslib.util.NameUtil;
import nl.sidn.pcap.PcapReader;
import nl.sidn.pcap.packet.Packet;
import nl.sidnlabs.entrada.config.Settings;
import nl.sidnlabs.entrada.dns.resolver.DnsResolverAddress;
import nl.sidnlabs.entrada.dns.resolver.GoogleResolverCheck;
import nl.sidnlabs.entrada.dns.resolver.OpenDNSResolverCheck;
import nl.sidnlabs.entrada.ip.geo.GeoIPService;
import nl.sidnlabs.entrada.metric.MetricManager;
import nl.sidnlabs.entrada.support.PacketCombination;

@Component
public class DNSParquetPacketWriter extends AbstractParquetPacketWriter {

  private static final int RCODE_QUERY_WITHOUT_RESPONSE = -1;

  private static final String DNS_AVRO_SCHEMA = "dns-query.avsc";

  // metrics
  private long responseBytes = 0;
  private long requestBytes = 0;
  private Map<Integer, Integer> qtypes = new HashMap<>();
  private Map<Integer, Integer> rcodes = new HashMap<>();
  private Map<Integer, Integer> opcodes = new HashMap<>();
  private int requestUDPFragmentedCount = 0;
  private int requestTCPFragmentedCount = 0;
  private int responseUDPFragmentedCount = 0;
  private int responseTCPFragmentedCount = 0;
  private int ipv4QueryCount = 0;
  private int ipv6QueryCount = 0;


  private Settings settings;
  private MetricManager metricManager;
  private DnsResolverAddress googleCheck;
  private DnsResolverAddress openDNSCheck;

  public DNSParquetPacketWriter(Settings settings, GoogleResolverCheck googleCheck,
      OpenDNSResolverCheck openDNSCheck, MetricManager metricManager, GeoIPService geoLookup) {

    super(geoLookup);
    this.settings = settings;
    this.googleCheck = googleCheck;
    this.openDNSCheck = openDNSCheck;
    this.metricManager = metricManager;
  }

  /**
   * Get the question, from the request packet if not available then from the response, which should
   * be the same.
   *
   * @param reqMessage
   * @param respMessage
   * @return
   */
  private Question lookupQuestion(Message reqMessage, Message respMessage) {
    if (reqMessage != null && !reqMessage.getQuestions().isEmpty()) {
      return reqMessage.getQuestions().get(0);
    } else if (respMessage != null && !respMessage.getQuestions().isEmpty()) {
      return respMessage.getQuestions().get(0);
    }
    // should never get here
    return null;
  }

  private long lookupTime(Packet reqPacket, Packet respPacket) {
    if (reqPacket != null) {
      return reqPacket.getTs();
    } else if (respPacket != null) {
      return respPacket.getTs();
    }
    // should never get here
    return -1;
  }

  /**
   * create 1 parquet record which combines values from the query and the response
   *
   * @param packet
   */
  @Override
  public void write(PacketCombination combo) {

    GenericRecordBuilder builder = recordBuilder(DNS_AVRO_SCHEMA);

    packetCounter++;
    if (packetCounter % STATUS_COUNT == 0) {
      showStatus();
    }
    Packet reqTransport = combo.getRequest();
    Message requestMessage = combo.getRequestMessage();
    Packet respTransport = combo.getResponse();
    Message respMessage = combo.getResponseMessage();

    // get the question
    Question question = lookupQuestion(requestMessage, respMessage);

    // get the headers from the messages.
    Header requestHeader = null;
    Header responseHeader = null;
    if (requestMessage != null) {
      requestHeader = requestMessage.getHeader();
    }
    if (respMessage != null) {
      responseHeader = respMessage.getHeader();
    }

    // get the time in milliseconds
    long time = lookupTime(reqTransport, respTransport);
    Timestamp ts = new Timestamp((time * 1000));

    // get the qname domain name details
    String normalizedQname = question == null ? "" : filter(question.getQName());
    normalizedQname = StringUtils.lowerCase(normalizedQname);
    Domaininfo domaininfo = NameUtil.getDomain(normalizedQname, settings.getTldSuffixes());
    // check to see it a response was found, if not then save -1 value
    // otherwise use the rcode returned by the server in the response.
    // no response might be caused by rate limiting
    int rcode = RCODE_QUERY_WITHOUT_RESPONSE; // default no reply, use non standard rcode value -1

    // set the nameserver the queries are going to/coming from
    builder.set("svr", combo.getServer().getName());

    // if no anycast location is encoded in the name then the anycast location will be null
    builder.set("server_location", combo.getServer().getLocation());

    // add file name, makes it easier to find the original input pcap
    // in case of of debugging.
    builder.set("pcap_file", combo.getPcapFilename());

    // add meta data
    enrich(reqTransport, respTransport, builder);

    // these are the values that are retrieved from the response
    if (respTransport != null && respMessage != null && responseHeader != null) {
      // use rcode from response
      rcode = responseHeader.getRawRcode();

      builder
          .set("id", responseHeader.getId())
          .set("opcode", responseHeader.getRawOpcode())
          .set("aa", responseHeader.isAa())
          .set("tc", responseHeader.isTc())
          .set("ra", responseHeader.isRa())
          .set("ad", responseHeader.isAd())
          .set("ancount", (int) responseHeader.getAnCount())
          .set("arcount", (int) responseHeader.getArCount())
          .set("nscount", (int) responseHeader.getNsCount())
          .set("qdcount", (int) responseHeader.getQdCount())
          // size of the complete packet incl all headers
          .set("res_len", respTransport.getTotalLength())
          // size of the dns message
          .set("dns_res_len", respMessage.getBytes());

      // ip fragments in the response
      if (respTransport.isFragmented()) {
        int frags = respTransport.getReassembledFragments();
        builder.set("resp_frag", frags);

        if ((respTransport.getProtocol() == PcapReader.PROTOCOL_UDP) && frags > 1) {
          responseUDPFragmentedCount++;
        } else if ((respTransport.getProtocol() == PcapReader.PROTOCOL_TCP) && frags > 1) {
          responseTCPFragmentedCount++;
        }
      }

      // EDNS0 for response
      writeResponseOptions(respMessage, builder);

      // update metric
      responseBytes = responseBytes + respTransport.getUdpLength();
      if (!combo.isExpired()) {
        // do not send expired queries, this will cause duplicate timestamps with low values
        // this looks like dips in the grafana graph
        metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_RESPONSE_COUNT, 1, time);
      }
    } // end of response only section

    // values from request OR response now
    // if no request found in the request then use values from the response.
    builder
        .set("rcode", rcode)
        .set("unixtime", reqTransport != null ? reqTransport.getTs() : respTransport.getTs())
        .set("time", ts.getTime())
        .set("time_micro",
            reqTransport != null ? reqTransport.getTsmicros() : respTransport.getTsmicros())
        .set("qname", normalizedQname)
        .set("domainname", domaininfo.getName())
        .set("labels", domaininfo.getLabels())
        .set("src", reqTransport != null ? reqTransport.getSrc() : respTransport.getDst())
        .set("len", reqTransport != null ? reqTransport.getTotalLength() : null)
        .set("ttl", reqTransport != null ? reqTransport.getTtl() : null)
        .set("ipv",
            reqTransport != null ? (int) reqTransport.getIpVersion()
                : (int) respTransport.getIpVersion())
        .set("prot",
            reqTransport != null ? (int) reqTransport.getProtocol()
                : (int) respTransport.getProtocol())
        .set("srcp", reqTransport != null ? reqTransport.getSrcPort() : null)
        .set("dst", reqTransport != null ? reqTransport.getDst() : respTransport.getSrc())
        .set("dstp", reqTransport != null ? reqTransport.getDstPort() : respTransport.getSrcPort())
        .set("udp_sum", reqTransport != null ? reqTransport.getUdpsum() : null)
        .set("dns_len", requestMessage != null ? requestMessage.getBytes() : null);

    // get values from the request only.
    // may overwrite values from the response
    if (reqTransport != null && requestHeader != null) {
      builder
          .set("id", requestHeader.getId())
          .set("opcode", requestHeader.getRawOpcode())
          .set("rd", requestHeader.isRd())
          .set("z", requestHeader.isZ())
          .set("cd", requestHeader.isCd())
          .set("qdcount", (int) requestHeader.getQdCount())
          .set("id", requestHeader.getId())
          .set("q_tc", requestHeader.isTc())
          .set("q_ra", requestHeader.isRa())
          .set("q_ad", requestHeader.isAd())
          .set("q_rcode", requestHeader.getRawRcode());

      // ip fragments in the request
      if (reqTransport.isFragmented()) {
        int requestFrags = reqTransport.getReassembledFragments();
        builder.set("frag", requestFrags);

        if ((reqTransport.getProtocol() == PcapReader.PROTOCOL_UDP) && requestFrags > 1) {
          requestUDPFragmentedCount++;
        } else if ((reqTransport.getProtocol() == PcapReader.PROTOCOL_TCP) && requestFrags > 1) {
          requestTCPFragmentedCount++;
        }
      } // end request only section

      // update metrics
      requestBytes = requestBytes + reqTransport.getUdpLength();
      if (!combo.isExpired()) {
        // do not send expired queries, this will cause duplicate timestamps with low values
        // this looks like dips in the grafana graph
        metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_QUERY_COUNT, 1, time);
      }
    }

    if (rcode == RCODE_QUERY_WITHOUT_RESPONSE) {
      // no response found for query, update stats
      metricManager.sendAggregated(MetricManager.METRIC_IMPORT_DNS_NO_RESPONSE_COUNT, 1, time);
    }

    // question
    writeQuestion(question, builder);

    // EDNS0 for request
    writeRequestOptions(requestMessage, builder);

    // calculate the processing time
    writeProctime(reqTransport, respTransport, builder);

    // create the actual record and write to parquet file
    GenericRecord record = builder.build();
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(ts.getTime());

    writer
        .write(record, schema(DNS_AVRO_SCHEMA), cal.get(Calendar.YEAR), cal.get(Calendar.MONTH),
            cal.get(Calendar.DAY_OF_MONTH), combo.getServer().getName());

    // create metrics
    updateMetricMap(rcodes, rcode);
    updateMetricMap(opcodes,
        requestHeader != null ? requestHeader.getRawOpcode() : responseHeader.getRawOpcode());
    // ip version stats
    updateIpVersionMetrics(reqTransport, respTransport);

    // if packet was expired and dropped from cache then increase stats for this
    if (combo.isExpired()) {
      metricManager
          .sendAggregated(MetricManager.METRIC_IMPORT_CACHE_EXPPIRED_DNS_QUERY_COUNT, 1, time,
              false);
    }
  }

  private void enrich(Packet reqPacket, Packet respPacket, GenericRecordBuilder builder) {
    Optional<String> country = null;
    Optional<String> asn = null;
    boolean isGoogle = false;
    boolean isOpenDNS = false;
    if (reqPacket != null) {
      // request packet, check the source address
      country = getCountry(reqPacket.getSrc());
      asn = getAsn(reqPacket.getSrc());
      isGoogle = googleCheck.isMatch(reqPacket.getSrc());
      if (!isGoogle) {
        isOpenDNS = openDNSCheck.isMatch(reqPacket.getSrc());
      }
    } else {
      // response packet, check the destination address
      country = getCountry(respPacket.getDst());
      asn = getAsn(respPacket.getDst());
      isGoogle = googleCheck.isMatch(respPacket.getDst());
      if (!isGoogle) {
        isOpenDNS = openDNSCheck.isMatch(respPacket.getDst());
      }
    }

    builder
        .set("country", country.orElse(null))
        .set("asn", asn.orElse(null))
        // check of the resolver is a google backend resolver
        .set("is_google", isGoogle)
        // check of the resolver is a opendns backend resolver
        .set("is_opendns", isOpenDNS);
  }

  private void updateIpVersionMetrics(Packet req, Packet resp) {
    if (req != null) {
      if (req.getIpVersion() == 4) {
        ipv4QueryCount++;
      } else {
        ipv6QueryCount++;
      }
    } else {
      if (resp.getIpVersion() == 4) {
        ipv4QueryCount++;
      } else {
        ipv6QueryCount++;
      }
    }
  }

  private void writeQuestion(Question q, GenericRecordBuilder builder) {
    if (q != null) {
      // unassigned, private or unknown, get raw value
      builder.set("qtype", q.getQTypeValue());
      // unassigned, private or unknown, get raw value
      builder.set("qclass", q.getQClassValue());
      // qtype metrics
      updateMetricMap(qtypes, q.getQTypeValue());
    }
  }


  // calc the number of seconds between receivinfg the response and sending it back to the resolver
  private void writeProctime(Packet reqTransport, Packet respTransport,
      GenericRecordBuilder builder) {
    if (reqTransport != null && respTransport != null) {
      Timestamp reqTs = new Timestamp((reqTransport.getTs() * 1000000));
      Timestamp respTs = new Timestamp((respTransport.getTs() * 1000000));

      // from second to microseconds
      long millis1 = respTs.getTime() - reqTs.getTime();
      long millis2 = (respTransport.getTsmicros() - reqTransport.getTsmicros());
      builder.set("proc_time", millis1 + millis2);
    }
  }

  /**
   * Write EDNS0 option (if any are present) to file.
   *
   * @param message
   * @param builder
   */
  private void writeResponseOptions(Message message, GenericRecordBuilder builder) {
    if (message == null) {
      return;
    }

    OPTResourceRecord opt = message.getPseudo();
    if (opt != null) {
      for (EDNS0Option option : opt.getOptions()) {
        if (option instanceof NSidOption) {
          String id = ((NSidOption) option).getId();
          builder.set("edns_nsid", id != null ? id : "");

          // this is the only server edns data we support, stop processing other options
          break;
        }
      }
    }

  }

  /**
   * Write EDNS0 option (if any are present) to file.
   *
   * @param message
   * @param builder
   */
  private void writeRequestOptions(Message message, GenericRecordBuilder builder) {
    if (message == null) {
      return;
    }

    OPTResourceRecord opt = message.getPseudo();
    if (opt != null) {
      builder
          .set("edns_udp", (int) opt.getUdpPlayloadSize())
          .set("edns_version", (int) opt.getVersion())
          .set("edns_do", opt.isDnssecDo())
          .set("edns_padding", -1); // use default no padding found

      List<Integer> otherEdnsOptions = new ArrayList<>();
      for (EDNS0Option option : opt.getOptions()) {
        if (option instanceof PingOption) {
          builder.set("edns_ping", true);
        } else if (option instanceof DNSSECOption) {
          if (option.getCode() == DNSSECOption.OPTION_CODE_DAU) {
            builder.set("edns_dnssec_dau", ((DNSSECOption) option).export());
          } else if (option.getCode() == DNSSECOption.OPTION_CODE_DHU) {
            builder.set("edns_dnssec_dhu", ((DNSSECOption) option).export());
          } else { // N3U
            builder.set("edns_dnssec_n3u", ((DNSSECOption) option).export());
          }
        } else if (option instanceof ClientSubnetOption) {
          ClientSubnetOption scOption = (ClientSubnetOption) option;
          // get client country and asn

          if (scOption.getInetAddress() != null) {
            InetAddress addr = scOption.getInetAddress();
            Optional<String> clientCountry = geoLookup.lookupCountry(addr);
            Optional<String> clientASN = geoLookup.lookupASN(addr);

            builder
                .set("edns_client_subnet_asn", clientASN.orElse(null))
                .set("edns_client_subnet_country", clientCountry.orElse(null));

          }
          builder.set("edns_client_subnet", scOption.export());


        } else if (option instanceof PaddingOption) {
          builder.set("edns_padding", ((PaddingOption) option).getLength());
        } else if (option instanceof KeyTagOption) {
          KeyTagOption kto = (KeyTagOption) option;
          builder.set("edns_keytag_count", kto.getKeytags().size());
          builder.set("edns_keytag_list", Joiner.on(",").join(kto.getKeytags()));
        } else {
          // other
          otherEdnsOptions.add(option.getCode());
        }
      }

      if (!otherEdnsOptions.isEmpty()) {
        builder.set("edns_other", Joiner.on(",").join(otherEdnsOptions));
      }
    }
  }

  @Override
  public void writeMetrics() {

    for (Map.Entry<Integer, Integer> entry : rcodes.entrySet()) {

      if (entry.getKey().intValue() == -1) {
        // pseudo rcode -1 means no reponse, not an official rcode
        metricManager
            .send(MetricManager.METRIC_IMPORT_DNS_RCODE + ".NO_RESPONSE.count",
                entry.getValue().intValue());
      } else {
        RcodeType type = RcodeType.fromValue(entry.getKey().intValue());
        metricManager
            .send(MetricManager.METRIC_IMPORT_DNS_RCODE + StringUtils.upperCase("." + type.name())
                + ".count", entry.getValue().intValue());
      }
    }

    // metricManager.send(MetricManager.METRIC_IMPORT_IP_COUNT, geo_ip_cache.size());
    metricManager.send(MetricManager.METRIC_IMPORT_COUNTRY_COUNT, countries.size());
    // metricManager.send(MetricManager.METRIC_IMPORT_ASN_COUNT, asn_cache.size());

    if (responseBytes > 0) {
      metricManager
          .send(MetricManager.METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE, (int) (responseBytes / 1024));
    } else {
      metricManager.send(MetricManager.METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE, 0);
    }
    if (requestBytes > 0) {
      metricManager
          .send(MetricManager.METRIC_IMPORT_DNS_QUERY_BYTES_SIZE, (int) (requestBytes / 1024));
    } else {
      metricManager.send(MetricManager.METRIC_IMPORT_DNS_QUERY_BYTES_SIZE, 0);
    }

    qtypes.entrySet().stream().forEach(e -> {
      ResourceRecordType type = ResourceRecordType.fromValue(e.getKey().intValue());
      metricManager
          .send(MetricManager.METRIC_IMPORT_DNS_QTYPE + StringUtils.upperCase("." + type.name())
              + ".count", e.getValue().intValue());
    });

    opcodes.entrySet().stream().forEach(e -> {
      OpcodeType type = OpcodeType.fromValue(e.getKey().intValue());
      metricManager
          .send(MetricManager.METRIC_IMPORT_DNS_OPCODE + StringUtils.upperCase("." + type.name())
              + ".count", e.getValue().intValue());
    });

    metricManager
        .send(MetricManager.METRIC_IMPORT_UDP_REQUEST_FRAGMENTED_COUNT, requestUDPFragmentedCount);
    metricManager
        .send(MetricManager.METRIC_IMPORT_TCP_REQUEST_FRAGMENTED_COUNT, requestTCPFragmentedCount);
    metricManager
        .send(MetricManager.METRIC_IMPORT_UDP_RESPONSE_FRAGMENTED_COUNT,
            responseUDPFragmentedCount);
    metricManager
        .send(MetricManager.METRIC_IMPORT_TCP_RESPONSE_FRAGMENTED_COUNT,
            responseTCPFragmentedCount);

    metricManager.send(MetricManager.METRIC_IMPORT_IP_VERSION_4_COUNT, ipv4QueryCount);
    metricManager.send(MetricManager.METRIC_IMPORT_IP_VERSION_6_COUNT, ipv6QueryCount);

  }

}


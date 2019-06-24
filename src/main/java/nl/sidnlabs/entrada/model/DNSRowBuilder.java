package nl.sidnlabs.entrada.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import nl.sidnlabs.dnslib.message.Header;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.message.Question;
import nl.sidnlabs.dnslib.message.records.edns0.ClientSubnetOption;
import nl.sidnlabs.dnslib.message.records.edns0.DNSSECOption;
import nl.sidnlabs.dnslib.message.records.edns0.EDNS0Option;
import nl.sidnlabs.dnslib.message.records.edns0.KeyTagOption;
import nl.sidnlabs.dnslib.message.records.edns0.NSidOption;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidnlabs.dnslib.message.records.edns0.PaddingOption;
import nl.sidnlabs.dnslib.message.records.edns0.PingOption;
import nl.sidnlabs.dnslib.types.OpcodeType;
import nl.sidnlabs.dnslib.types.RcodeType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.dnslib.util.Domaininfo;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.metric.MetricManager;
import nl.sidnlabs.entrada.support.PacketCombination;
import nl.sidnlabs.pcap.PcapReader;
import nl.sidnlabs.pcap.packet.Packet;

/**
 * Create output format independent row model
 *
 */
@Component("dnsBuilder")
public class DNSRowBuilder extends AbstractRowBuilder {

  private static final int RCODE_QUERY_WITHOUT_RESPONSE = -1;

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

  int udp = 0;
  int tcp = 0;

  private MetricManager metricManager;

  public DNSRowBuilder(List<AddressEnrichment> enrichments, MetricManager metricManager) {
    super(enrichments);
    this.metricManager = metricManager;
  }

  @Override
  public Row build(PacketCombination combo) {

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
    Row row = new Row(ts);

    // get the qname domain name details
    String normalizedQname = question == null ? "" : filter(question.getQName());
    normalizedQname = StringUtils.lowerCase(normalizedQname);
    Domaininfo domaininfo = NameUtil.getDomain(normalizedQname);
    // check to see it a response was found, if not then save -1 value
    // otherwise use the rcode returned by the server in the response.
    // no response might be caused by rate limiting
    int rcode = RCODE_QUERY_WITHOUT_RESPONSE; // default no reply, use non standard rcode value -1

    // if no anycast location is encoded in the name then the anycast location will be null
    row.addColumn(column("server_location", combo.getServer().getLocation()));

    // add file name, makes it easier to find the original input pcap
    // in case of of debugging.
    row.addColumn(column("pcap_file", combo.getPcapFilename()));

    // add meta data for the client IP
    String addressToCheck = reqTransport != null ? reqTransport.getSrc() : respTransport.getDst();

    enrich(addressToCheck, "", row);

    // these are the values that are retrieved from the response
    if (respTransport != null && respMessage != null && responseHeader != null) {
      // use rcode from response
      rcode = responseHeader.getRawRcode();

      row
          .addColumn(column("id", responseHeader.getId()))
          .addColumn(column("opcode", responseHeader.getRawOpcode()))
          .addColumn(column("aa", responseHeader.isAa()))
          .addColumn(column("tc", responseHeader.isTc()))
          .addColumn(column("ra", responseHeader.isRa()))
          .addColumn(column("ad", responseHeader.isAd()))
          .addColumn(column("ancount", (int) responseHeader.getAnCount()))
          .addColumn(column("arcount", (int) responseHeader.getArCount()))
          .addColumn(column("nscount", (int) responseHeader.getNsCount()))
          .addColumn(column("qdcount", (int) responseHeader.getQdCount()))
          // size of the complete packet incl all headers
          .addColumn(column("res_len", respTransport.getTotalLength()))
          // size of the dns message
          .addColumn(column("dns_res_len", respMessage.getBytes()));

      // ip fragments in the response
      if (respTransport.isFragmented()) {
        int frags = respTransport.getReassembledFragments();
        row.addColumn(column("resp_frag", frags));

        if ((respTransport.getProtocol() == PcapReader.PROTOCOL_UDP) && frags > 1) {
          responseUDPFragmentedCount++;
        } else if ((respTransport.getProtocol() == PcapReader.PROTOCOL_TCP) && frags > 1) {
          responseTCPFragmentedCount++;
        }
      }

      // EDNS0 for response
      writeResponseOptions(respMessage, row);

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
    row
        .addColumn(column("rcode", rcode))
        .addColumn(column("unixtime", time(reqTransport, respTransport)))
        .addColumn(column("time", ts.getTime()))
        .addColumn(column("time_micro",
            reqTransport != null ? reqTransport.getTsmicros() : respTransport.getTsmicros()))
        .addColumn(column("qname", normalizedQname))
        .addColumn(column("domainname", domaininfo.getName()))
        .addColumn(column("labels", domaininfo.getLabels()))
        .addColumn(
            column("src", reqTransport != null ? reqTransport.getSrc() : respTransport.getDst()))
        .addColumn(column("ipv",
            reqTransport != null ? (int) reqTransport.getIpVersion()
                : (int) respTransport.getIpVersion()))
        .addColumn(column("prot",
            reqTransport != null ? (int) reqTransport.getProtocol()
                : (int) respTransport.getProtocol()))

        .addColumn(
            column("dst", reqTransport != null ? reqTransport.getDst() : respTransport.getSrc()))
        .addColumn(column("dstp",
            reqTransport != null ? reqTransport.getDstPort() : respTransport.getSrcPort()));

    if (reqTransport != null) {

      // metrics
      if (reqTransport.getProtocol() == PcapReader.PROTOCOL_UDP) {
        udp++;
      } else {
        tcp++;
      }

      row.addColumn(column("udp_sum", reqTransport.getUdpsum()));
      row.addColumn(column("srcp", reqTransport.getSrcPort()));
      row.addColumn(column("len", reqTransport.getTotalLength()));
      row.addColumn(column("ttl", reqTransport.getTtl()));
    }
    if (requestMessage != null) {
      row.addColumn(column("dns_len", requestMessage.getBytes()));
    }

    // get values from the request only.
    // may overwrite values from the response
    if (reqTransport != null && requestHeader != null) {
      row
          .addColumn(column("id", requestHeader.getId()))
          .addColumn(column("opcode", requestHeader.getRawOpcode()))
          .addColumn(column("rd", requestHeader.isRd()))
          .addColumn(column("z", requestHeader.isZ()))
          .addColumn(column("cd", requestHeader.isCd()))
          .addColumn(column("qdcount", (int) requestHeader.getQdCount()))
          .addColumn(column("id", requestHeader.getId()))
          .addColumn(column("q_tc", requestHeader.isTc()))
          .addColumn(column("q_ra", requestHeader.isRa()))
          .addColumn(column("q_ad", requestHeader.isAd()))
          .addColumn(column("q_rcode", requestHeader.getRawRcode()));

      // ip fragments in the request
      if (reqTransport.isFragmented()) {
        int requestFrags = reqTransport.getReassembledFragments();
        row.addColumn(column("frag", requestFrags));

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
    writeQuestion(question, row);

    // EDNS0 for request
    writeRequestOptions(requestMessage, row);

    // calculate the processing time
    writeProctime(reqTransport, respTransport, row);

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


    return row;
  }

  private long time(Packet reqTransport, Packet respTransport) {
    return (reqTransport != null) ? reqTransport.getTs() : respTransport.getTs();
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


  private void writeQuestion(Question q, Row row) {
    if (q != null) {
      // unassigned, private or unknown, get raw value
      row.addColumn(column("qtype", q.getQTypeValue()));
      // unassigned, private or unknown, get raw value
      row.addColumn(column("qclass", q.getQClassValue()));
      // qtype metrics
      updateMetricMap(qtypes, q.getQTypeValue());
    }
  }

  /**
   * Write EDNS0 option (if any are present) to file.
   *
   * @param message
   * @param builder
   */
  private void writeResponseOptions(Message message, Row row) {
    if (message == null) {
      return;
    }

    OPTResourceRecord opt = message.getPseudo();
    if (opt != null) {
      for (EDNS0Option option : opt.getOptions()) {
        if (option instanceof NSidOption) {
          String id = ((NSidOption) option).getId();
          row.addColumn(column("edns_nsid", id != null ? id : ""));

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
  private void writeRequestOptions(Message message, Row row) {
    if (message == null) {
      return;
    }

    OPTResourceRecord opt = message.getPseudo();
    if (opt != null) {
      row
          .addColumn(column("edns_udp", (int) opt.getUdpPlayloadSize()))
          .addColumn(column("edns_version", (int) opt.getVersion()))
          .addColumn(column("edns_do", opt.isDnssecDo()))
          .addColumn(column("edns_padding", -1)); // use default no padding found

      List<Integer> otherEdnsOptions = new ArrayList<>();
      for (EDNS0Option option : opt.getOptions()) {
        if (option instanceof PingOption) {
          row.addColumn(column("edns_ping", true));
        } else if (option instanceof DNSSECOption) {
          if (option.getCode() == DNSSECOption.OPTION_CODE_DAU) {
            row.addColumn(column("edns_dnssec_dau", ((DNSSECOption) option).export()));
          } else if (option.getCode() == DNSSECOption.OPTION_CODE_DHU) {
            row.addColumn(column("edns_dnssec_dhu", ((DNSSECOption) option).export()));
          } else { // N3U
            row.addColumn(column("edns_dnssec_n3u", ((DNSSECOption) option).export()));
          }
        } else if (option instanceof ClientSubnetOption) {
          ClientSubnetOption scOption = (ClientSubnetOption) option;
          // get client country and asn

          if (scOption.getAddress() != null) {
            enrich(scOption.getAddress(), "edns_client_subnet_", row);
          }
          row.addColumn(column("edns_client_subnet", scOption.export()));


        } else if (option instanceof PaddingOption) {
          row.addColumn(column("edns_padding", ((PaddingOption) option).getLength()));
        } else if (option instanceof KeyTagOption) {
          KeyTagOption kto = (KeyTagOption) option;
          row.addColumn(column("edns_keytag_count", kto.getKeytags().size()));

          if (!kto.getKeytags().isEmpty()) {
            row
                .addColumn(column("edns_keytag_list",
                    kto
                        .getKeytags()
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","))));
          }
        } else {
          // other
          otherEdnsOptions.add(option.getCode());
        }
      }

      if (!otherEdnsOptions.isEmpty()) {
        row
            .addColumn(column("edns_other",
                otherEdnsOptions.stream().map(Object::toString).collect(Collectors.joining(","))));
      }
    }
  }



  // calc the number of seconds between receivinfg the response and sending it back to the resolver
  private void writeProctime(Packet reqTransport, Packet respTransport, Row row) {
    if (reqTransport != null && respTransport != null) {
      Timestamp reqTs = new Timestamp((reqTransport.getTs() * 1000000));
      Timestamp respTs = new Timestamp((respTransport.getTs() * 1000000));

      // from second to microseconds
      long millis1 = respTs.getTime() - reqTs.getTime();
      long millis2 = (respTransport.getTsmicros() - reqTransport.getTsmicros());
      row.addColumn(column("proc_time", millis1 + millis2));
    }
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

    int bytes = responseBytes == 0 ? 0 : (int) (responseBytes / 1024);
    metricManager.send(MetricManager.METRIC_IMPORT_DNS_RESPONSE_BYTES_SIZE, bytes);

    bytes = requestBytes == 0 ? 0 : (int) (requestBytes / 1024);
    metricManager.send(MetricManager.METRIC_IMPORT_DNS_QUERY_BYTES_SIZE, bytes);

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

    metricManager.send(MetricManager.METRIC_IMPORT_TCP_COUNT, tcp);
    metricManager.send(MetricManager.METRIC_IMPORT_UDP_COUNT, udp);

  }

}

package nl.sidnlabs.entrada.model;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.cache2k.Cache2kBuilder;
import org.springframework.beans.factory.annotation.Value;
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
import nl.sidnlabs.dnslib.util.Domaininfo;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;

/**
 * Create output format independent row model
 *
 */
@Component("dnsBuilder")
public class DNSRowBuilder extends AbstractRowBuilder {

  private static final int RCODE_QUERY_WITHOUT_RESPONSE = -1;
  private final static int CACHE_MAX_SIZE = 25000;

  private ServerContext serverCtx;

  @Value("${entrada.privacy.enabled:false}")
  private boolean privacy;

  public DNSRowBuilder(List<AddressEnrichment> enrichments, ServerContext serverCtx,
      HistoricalMetricManager metricManager) {
    super(enrichments, metricManager);
    this.serverCtx = serverCtx;

    cache = new Cache2kBuilder<String, Domaininfo>() {}
        .name("dns-domaininfo-cache")
        .entryCapacity(CACHE_MAX_SIZE)
        .build();
  }

  @Override
  public Row build(RowData combo, String server) {

    packetCounter++;
    if (packetCounter % STATUS_COUNT == 0) {
      showStatus();
    }

    Packet reqTransport = combo.getRequest();
    Message reqMessage = combo.getRequestMessage();
    Packet rspTransport = combo.getResponse();
    Message rspMessage = combo.getResponseMessage();

    // get the time in milliseconds
    long time = packetTime(reqTransport, rspTransport);
    Row row = new Row(new Timestamp(time));

    // get the question
    Question question = lookupQuestion(reqMessage, rspMessage);

    // get the headers from the messages.
    Header requestHeader = null;
    Header responseHeader = null;

    if (reqMessage != null) {
      requestHeader = reqMessage.getHeader();

      row.addColumn(column("req_len", reqMessage.getBytes()));
    }
    if (rspMessage != null) {
      responseHeader = rspMessage.getHeader();

      row.addColumn(column("res_len", rspMessage.getBytes()));
    }

    // get the qname domain name details
    String normalizedQname = question == null ? "" : filter(question.getQName());
    Domaininfo domaininfo = cache.peek(normalizedQname);
    if (domaininfo == null) {
      domaininfo = NameUtil.getDomain(normalizedQname, true);
      cache.put(normalizedQname, domaininfo);
    } else {
      domaininfoCacheHits++;
    }

    // check to see it a response was found, if not then use -1 value for rcode
    // otherwise use the rcode returned by the server in the response.
    // no response might be caused by rate limiting
    int rcode = RCODE_QUERY_WITHOUT_RESPONSE;

    // if no anycast location is encoded in the name then the anycast location will be null
    row.addColumn(column("server_location", serverCtx.getServerInfo().getLocation()));

    // add file name, makes it easier to find the original input pcap
    // in case of of debugging.
    row.addColumn(column("pcap_file", combo.getPcapFilename()));

    // add meta data for the client IP
    if (reqTransport != null && reqTransport.getSrcAddr() != null) {
      enrich(reqTransport.getSrc(), reqTransport.getSrcAddr(), "", row);
    } else if (rspTransport != null && rspTransport.getDstAddr() != null) {
      enrich(rspTransport.getDst(), rspTransport.getDstAddr(), "", row);
    }

    if (reqTransport != null) {
      // only add IP DF flag for server response packet
      row.addColumn(column("req_ip_df", reqTransport.isDoNotFragment()));

      if (reqTransport.getTcpHandshake() != null) {
        // found tcp handshake info
        row.addColumn(column("tcp_hs_rtt", reqTransport.getTcpHandshake().rtt()));
        metricManager
            .record(HistoricalMetricManager.METRIC_IMPORT_TCP_HANDSHAKE_RTT,
                (int) reqTransport.getTcpHandshake().rtt(), time, false);
      }
    }

    if (rspTransport != null) {
      // only add IP DF flag for server response packet
      row.addColumn(column("res_ip_df", rspTransport.isDoNotFragment()));

      // these are the values that are retrieved from the response
      if (rspMessage != null && responseHeader != null) {
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
            .addColumn(column("qdcount", (int) responseHeader.getQdCount()));

        // ip fragments in the response
        if (rspTransport.isFragmented()) {
          int frags = rspTransport.getReassembledFragments();
          row.addColumn(column("resp_frag", frags));
        }

        // EDNS0 for response
        writeResponseOptions(rspMessage, row);

        metricManager.record(HistoricalMetricManager.METRIC_IMPORT_DNS_RESPONSE_COUNT, 1, time);
      } // end of response only section
    }

    // values from request OR response now
    // if no request found in the request then use values from the response.
    row
        .addColumn(column("rcode", rcode))
        .addColumn(column("time", time))
        .addColumn(column("qname", normalizedQname))
        .addColumn(column("domainname", domaininfo.getName()))
        .addColumn(column("labels", domaininfo.getLabels()))
        .addColumn(column("ipv", ipVersion(reqTransport, rspTransport)))

        .addColumn(column("dst", dstIpAdress(reqTransport, rspTransport)))
        .addColumn(column("dstp", dstPort(reqTransport, rspTransport)))
        .addColumn(column("srcp", srcPort(reqTransport, rspTransport)));

    if (!privacy) {
      row.addColumn(column("src", srcIpAdress(reqTransport, rspTransport)));
    }

    int prot = protocol(reqTransport, rspTransport);
    row.addColumn(column("prot", prot));
    int opcode = opCode(requestHeader, responseHeader);

    // get values from the request only.
    // may overwrite values from the response
    if (reqTransport != null && requestHeader != null) {
      row
          .addColumn(column("ttl", reqTransport.getTtl()))
          .addColumn(column("id", requestHeader.getId()))
          .addColumn(column("opcode", opcode))
          .addColumn(column("rd", requestHeader.isRd()))
          .addColumn(column("z", requestHeader.isZ()))
          .addColumn(column("cd", requestHeader.isCd()))
          .addColumn(column("qdcount", (int) requestHeader.getQdCount()))
          .addColumn(column("id", requestHeader.getId()))
          .addColumn(column("q_tc", requestHeader.isTc()))
          .addColumn(column("q_ra", requestHeader.isRa()))
          .addColumn(column("q_ad", requestHeader.isAd()));

      // ip fragments in the request
      if (reqTransport.isFragmented()) {
        int requestFrags = reqTransport.getReassembledFragments();
        row.addColumn(column("frag", requestFrags));
      } // end request only section

      metricManager.record(HistoricalMetricManager.METRIC_IMPORT_DNS_QUERY_COUNT, 1, time);
    }

    // question
    if (question != null) {
      writeQuestion(question, row);

      metricManager
          .record(HistoricalMetricManager.METRIC_IMPORT_DNS_QTYPE + "." + question.getQTypeValue(),
              1, time);
    }

    // EDNS0 for request
    writeRequestOptions(reqMessage, row);

    // calculate the processing time
    if (reqTransport != null && rspTransport != null) {
      row.addColumn(column("proc_time", rspTransport.getTsMilli() - reqTransport.getTsMilli()));
    }

    // create metrics
    metricManager.record(HistoricalMetricManager.METRIC_IMPORT_DNS_RCODE + "." + rcode, 1, time);
    metricManager.record(HistoricalMetricManager.METRIC_IMPORT_DNS_OPCODE + "." + opcode, 1, time);


    if (reqTransport != null) {
      if (reqTransport.getIpVersion() == 4) {
        metricManager.record(HistoricalMetricManager.METRIC_IMPORT_IP_VERSION_4_COUNT, 1, time);
      } else {
        metricManager.record(HistoricalMetricManager.METRIC_IMPORT_IP_VERSION_6_COUNT, 1, time);
      }
    } else if (rspTransport != null) {
      if (rspTransport.getIpVersion() == 4) {
        metricManager.record(HistoricalMetricManager.METRIC_IMPORT_IP_VERSION_4_COUNT, 1, time);
      } else {
        metricManager.record(HistoricalMetricManager.METRIC_IMPORT_IP_VERSION_6_COUNT, 1, time);
      }
    }

    if (prot == PacketFactory.PROTOCOL_TCP) {
      metricManager.record(HistoricalMetricManager.METRIC_IMPORT_TCP_COUNT, 1, time);
    } else {
      metricManager.record(HistoricalMetricManager.METRIC_IMPORT_UDP_COUNT, 1, time);
    }

    return row;
  }

  private int opCode(Header req, Header rsp) {

    if (req != null) {
      return req.getRawOpcode();
    }

    if (rsp != null) {
      return rsp.getRawOpcode();
    }

    return -1;
  }

  private int srcPort(Packet reqTransport, Packet respTransport) {
    return reqTransport != null ? reqTransport.getSrcPort() : respTransport.getDstPort();
  }

  private int dstPort(Packet reqTransport, Packet respTransport) {
    return reqTransport != null ? reqTransport.getDstPort() : respTransport.getSrcPort();
  }


  private int protocol(Packet reqTransport, Packet respTransport) {
    return reqTransport != null ? (int) reqTransport.getProtocol()
        : (int) respTransport.getProtocol();
  }

  private int ipVersion(Packet reqTransport, Packet respTransport) {
    return reqTransport != null ? (int) reqTransport.getIpVersion()
        : (int) respTransport.getIpVersion();
  }

  private String dstIpAdress(Packet reqTransport, Packet respTransport) {
    return reqTransport != null ? reqTransport.getDst() : respTransport.getSrc();
  }

  private String srcIpAdress(Packet reqTransport, Packet respTransport) {
    return reqTransport != null ? reqTransport.getSrc() : respTransport.getDst();
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

  private long packetTime(Packet reqPacket, Packet respTransport) {
    return (reqPacket != null) ? reqPacket.getTsMilli() : respTransport.getTsMilli();
  }


  private void writeQuestion(Question q, Row row) {
    // unassigned, private or unknown, get raw value
    row.addColumn(column("qtype", q.getQTypeValue()));
    // unassigned, private or unknown, get raw value
    row.addColumn(column("qclass", q.getQClassValue()));
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
          .addColumn(column("edns_do", opt.isDnssecDo()));

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
            enrich(scOption.getAddress(), scOption.getInetAddress(), "edns_client_subnet_", row);
          }
          if (!privacy) {
            row.addColumn(column("edns_client_subnet", scOption.export()));
          }


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

}

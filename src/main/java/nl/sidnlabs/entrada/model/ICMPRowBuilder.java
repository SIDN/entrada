package nl.sidnlabs.entrada.model;

import java.sql.Timestamp;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import nl.sidnlabs.dnslib.message.Header;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.message.Question;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidnlabs.dnslib.util.Domaininfo;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.metric.HistoricalMetricManager;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.ICMPPacket;
import nl.sidnlabs.pcap.packet.Packet;

@Component("icmpBuilder")
public class ICMPRowBuilder extends AbstractRowBuilder {

  private ServerContext serverCtx;

  @Value("${entrada.privacy.enabled:false}")
  private boolean privacy;

  public ICMPRowBuilder(List<AddressEnrichment> enrichments, ServerContext serverCtx,
      HistoricalMetricManager metricManager) {
    super(enrichments, metricManager);
    this.serverCtx = serverCtx;
  }

  @Override
  public Row build(RowData combo, String server) {

    ICMPPacket icmpPacket = (ICMPPacket) combo.getRequest();

    Packet originalPacket = null;
    Message dnsResponseMessage = null;
    ICMPPacket originalICMPPacket = null;

    packetCounter++;
    if (packetCounter % STATUS_COUNT == 0) {
      showStatus();
    }

    // get message .nl auth send to client and which is returned in the icmp payload
    if (icmpPacket.getOriginalIPPacket() != null
        && icmpPacket.getOriginalIPPacket() != Packet.NULL) {
      originalPacket = icmpPacket.getOriginalIPPacket();
    }

    if (originalPacket instanceof DNSPacket) {
      dnsResponseMessage = ((DNSPacket) originalPacket).getMessage();
    } else if (originalPacket instanceof ICMPPacket) {
      originalICMPPacket = (ICMPPacket) originalPacket;
    }


    // get the time in milliseconds
    Row row = new Row(new Timestamp(icmpPacket.getTsMilli()));

    enrich(icmpPacket.getSrc(), "ip_", row);

    // icmp payload
    Question q = null;
    Header dnsResponseHdr = null;
    Domaininfo domaininfo = null;
    String normalizedQname = null;

    if (dnsResponseMessage != null) {
      // malformed (bad_format) message can have missing question
      if (!dnsResponseMessage.getQuestions().isEmpty()) {
        q = dnsResponseMessage.getQuestions().get(0);
      }
      dnsResponseHdr = dnsResponseMessage.getHeader();
      normalizedQname = q == null ? "" : filter(q.getQName());
      normalizedQname = StringUtils.lowerCase(normalizedQname);
      domaininfo = NameUtil.getDomain(normalizedQname);
    }

    // values from query now.
    row
        .addColumn(column("time", icmpPacket.getTsMilli()))
        .addColumn(column("icmp_type", icmpPacket.getType()))
        .addColumn(column("icmp_code", icmpPacket.getCode()))
        .addColumn(column("icmp_echo_client_type", icmpPacket.getClientType()))
        .addColumn(column("ip_ttl", icmpPacket.getTtl()))
        .addColumn(column("ip_v", (int) icmpPacket.getIpVersion()))
        .addColumn(column("ip_dst", icmpPacket.getDst()))
        .addColumn(column("l4_prot", (int) icmpPacket.getProtocol()))
        .addColumn(column("l4_srcp", icmpPacket.getSrcPort()))
        .addColumn(column("l4_dstp", icmpPacket.getDstPort()))
        // size of ip packet incl headers
        .addColumn(column("ip_len", icmpPacket.getTotalLength()));

    if (icmpPacket.getMtu() > 0) {
      row.addColumn(column("icmp_ip_mtu", icmpPacket.getMtu()));
    }

    if (!privacy) {
      row.addColumn(column("ip_src", icmpPacket.getSrc()));
    }
    // add file name
    row.addColumn(column("pcap_file", combo.getPcapFilename()));

    row.addColumn(column("server", server));
    // if no anycast location is encoded in the name then the anycast server name and location will
    // be null
    // only store this column in case of anycast, to save storage space.
    // the server name can be determined with the "svr" column
    row.addColumn(column("server_location", serverCtx.getServerInfo().getLocation()));

    // orig packet from payload

    if (originalPacket != null && originalPacket != Packet.NULL) {

      row
          .addColumn(column("orig_ip_ttl", originalPacket.getTtl()))
          .addColumn(column("orig_ip_v", (int) originalPacket.getIpVersion()))
          .addColumn(column("orig_ip_dst", originalPacket.getDst()))
          .addColumn(column("orig_l4_prot", (int) originalPacket.getProtocol()))
          .addColumn(column("orig_l4_srcp", originalPacket.getSrcPort()))
          .addColumn(column("orig_l4_dstp", originalPacket.getDstPort()))
          // size of ip packet incl headers
          .addColumn(column("orig_ip_len", originalPacket.getTotalLength()));

      if (!privacy) {
        row.addColumn(column("orig_ip_src", originalPacket.getSrc()));
      }

      if (originalICMPPacket != null) {
        row
            .addColumn(column("orig_icmp_type", originalICMPPacket.getType()))
            .addColumn(column("orig_icmp_code", originalICMPPacket.getCode()))
            .addColumn(column("orig_icmp_echo_client_type", originalICMPPacket.getClientType()));
      }

      if (dnsResponseMessage != null) {
        // orig dns response from icmp packet
        row
            .addColumn(column("orig_dns_len", originalPacket.getPayloadLength())) // get the size
                                                                                  // from the
            // reassembled udp header of
            // the original udp response
            .addColumn(column("orig_dns_id", dnsResponseHdr.getId()))
            .addColumn(column("orig_dns_qname", normalizedQname))
            .addColumn(column("orig_dns_domainname", domaininfo.getName()))
            .addColumn(column("orig_dns_aa", dnsResponseHdr.isAa()))
            .addColumn(column("orig_dns_tc", dnsResponseHdr.isTc()))
            .addColumn(column("orig_dns_rd", dnsResponseHdr.isRd()))
            .addColumn(column("orig_dns_ra", dnsResponseHdr.isRa()))
            .addColumn(column("orig_dns_z", dnsResponseHdr.isZ()))
            .addColumn(column("orig_dns_ad", dnsResponseHdr.isAd()))
            .addColumn(column("orig_dns_cd", dnsResponseHdr.isCd()))
            .addColumn(column("orig_dns_ancount", (int) dnsResponseHdr.getAnCount()))
            .addColumn(column("orig_dns_arcount", (int) dnsResponseHdr.getArCount()))
            .addColumn(column("orig_dns_nscount", (int) dnsResponseHdr.getNsCount()))
            .addColumn(column("orig_dns_qdcount", (int) dnsResponseHdr.getQdCount()))
            .addColumn(column("orig_dns_rcode", dnsResponseHdr.getRawRcode()))
            .addColumn(column("orig_dns_opcode", dnsResponseHdr.getRawOpcode()))
            .addColumn(column("orig_dns_labels", domaininfo.getLabels()));

        if (q != null) {
          // unassinged, private or unknown, get raw value
          row.addColumn(column("orig_dns_qtype", q.getQTypeValue()));
          // unassinged, private or unknown, get raw value
          row.addColumn(column("orig_dns_qclass", q.getQClassValue()));
        }

        OPTResourceRecord opt = dnsResponseMessage.getPseudo();
        if (opt != null) {
          row
              .addColumn(column("orig_dns_edns_udp", (int) opt.getUdpPlayloadSize()))
              .addColumn(column("orig_dns_edns_version", (int) opt.getVersion()))
              .addColumn(column("orig_dns_edns_do", opt.isDnssecDo()));
        }
      }
    }

    return row;
  }

}

package nl.sidnlabs.entrada.model;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.stereotype.Component;
import akka.japi.Pair;
import nl.sidnlabs.dnslib.message.Header;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.message.Question;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.entrada.ServerContext;
import nl.sidnlabs.entrada.enrich.AddressEnrichment;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.ICMPPacket;
import nl.sidnlabs.pcap.packet.Packet;

@Component("icmp")
public class ICMPRowBuilder extends AbstractRowBuilder {

  private static final String ICMP_AVRO_SCHEMA = "/avro/icmp-packet.avsc";

  private Schema schema = schema(ICMP_AVRO_SCHEMA);

  public ICMPRowBuilder(List<AddressEnrichment> enrichments, ServerContext serverCtx) {
    super(enrichments, serverCtx);
  }

  @Override
  public Pair<GenericRecord, BaseMetricValues> build(RowData combo, String server) {
    throw new NotImplementedException();
  }

  @Override
  public Pair<GenericRecord, BaseMetricValues> build(Packet p, String server) {

    GenericRecord record = new GenericData.Record(schema);

    ICMPPacket icmpPacket = (ICMPPacket) p;

    IcmpMetricValues mv = new IcmpMetricValues(icmpPacket.getTsMilli());

    Packet originalPacket = null;
    Message dnsResponseMessage = null;
    ICMPPacket originalICMPPacket = null;

    counter++;
    if (counter % STATUS_COUNT == 0) {
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

    enrich(icmpPacket.getSrc(), icmpPacket.getSrcAddr(), "ip_", record, false);

    // icmp payload
    Question q = null;
    Header dnsResponseHdr = null;
    String qname = null;
    String domainname = null;
    int labels = 0;

    if (dnsResponseMessage != null) {
      // malformed (bad_format) message can have missing question
      if (!dnsResponseMessage.getQuestions().isEmpty()) {
        q = dnsResponseMessage.getQuestions().get(0);
      }
      qname = q != null ? q.getQName() : null;
      if (qname != null) {
        domainname = domainCache.peek(qname);
        labels = NameUtil.labels(qname);
      }
      if (domainname == null && qname != null) {
        domainname = NameUtil.domainname(qname);
        if (domainname != null) {
          domainCache.putIfAbsent(qname, domainname);
        }
      } else {
        domainCacheHits++;
      }

      dnsResponseHdr = dnsResponseMessage.getHeader();
    }

    // values from query now.

    record.put("time", Long.valueOf(icmpPacket.getTsMilli()));
    record.put("icmp_type", Integer.valueOf(icmpPacket.getType()));
    record.put("icmp_code", Integer.valueOf(icmpPacket.getCode()));
    record.put("icmp_echo_client_type", Integer.valueOf(icmpPacket.getClientType()));
    record.put("ip_ttl", Integer.valueOf(icmpPacket.getTtl()));
    record.put("ip_v", Integer.valueOf(icmpPacket.getIpVersion()));
    record.put("ip_dst", icmpPacket.getDst());
    record.put("l4_prot", Integer.valueOf(icmpPacket.getProtocol()));
    record.put("l4_srcp", Integer.valueOf(icmpPacket.getSrcPort()));
    record.put("l4_dstp", Integer.valueOf(icmpPacket.getDstPort()));
    // size of ip packet incl headers
    record.put("ip_len", Integer.valueOf(icmpPacket.getTotalLength()));

    if (icmpPacket.getMtu() > 0) {
      record.put("icmp_ip_mtu", Integer.valueOf(icmpPacket.getMtu()));
    }

    if (!privacy) {
      record.put("ip_src", icmpPacket.getSrc());
    }
    // add file name
    record.put("pcap_file", p.getFilename());

    record.put("server", server);
    // if no anycast location is encoded in the name then the anycast server name and location will
    // be null
    // only store this column in case of anycast, to save storage space.
    // the server name can be determined with the "svr" column
    record.put("server_location", serverCtx.getServerInfo().getLocation());

    // orig packet from payload

    if (originalPacket != null && originalPacket != Packet.NULL) {

      record.put("orig_ip_ttl", Integer.valueOf(originalPacket.getTtl()));
      record.put("orig_ip_v", Integer.valueOf(originalPacket.getIpVersion()));
      record.put("orig_ip_dst", originalPacket.getDst());
      record.put("orig_l4_prot", Integer.valueOf(originalPacket.getProtocol()));
      record.put("orig_l4_srcp", Integer.valueOf(originalPacket.getSrcPort()));
      record.put("orig_l4_dstp", Integer.valueOf(originalPacket.getDstPort()));
      // size of ip packet incl headers
      record.put("orig_ip_len", Integer.valueOf(originalPacket.getTotalLength()));

      if (!privacy) {
        record.put("orig_ip_src", originalPacket.getSrc());
      }

      if (originalICMPPacket != null) {

        record.put("orig_icmp_type", Integer.valueOf(originalICMPPacket.getType()));
        record.put("orig_icmp_code", Integer.valueOf(originalICMPPacket.getCode()));
        record
            .put("orig_icmp_echo_client_type", Integer.valueOf(originalICMPPacket.getClientType()));
      }

      if (dnsResponseMessage != null) {
        // orig dns response from icmp packet

        record.put("orig_dns_len", Integer.valueOf(originalPacket.getPayloadLength())); // get the
                                                                                        // size
        // from the
        // reassembled udp header of
        // the original udp response
        record.put("orig_dns_id", Integer.valueOf(dnsResponseHdr.getId()));
        record.put("orig_dns_qname", qname);
        record.put("orig_dns_domainname", domainname);
        record.put("orig_dns_aa", Boolean.valueOf(dnsResponseHdr.isAa()));
        record.put("orig_dns_tc", Boolean.valueOf(dnsResponseHdr.isTc()));
        record.put("orig_dns_rd", Boolean.valueOf(dnsResponseHdr.isRd()));
        record.put("orig_dns_ra", Boolean.valueOf(dnsResponseHdr.isRa()));
        record.put("orig_dns_z", Boolean.valueOf(dnsResponseHdr.isZ()));
        record.put("orig_dns_ad", Boolean.valueOf(dnsResponseHdr.isAd()));
        record.put("orig_dns_cd", Boolean.valueOf(dnsResponseHdr.isCd()));
        record.put("orig_dns_ancount", Integer.valueOf(dnsResponseHdr.getAnCount()));
        record.put("orig_dns_arcount", Integer.valueOf(dnsResponseHdr.getArCount()));
        record.put("orig_dns_nscount", Integer.valueOf(dnsResponseHdr.getNsCount()));
        record.put("orig_dns_qdcount", Integer.valueOf(dnsResponseHdr.getQdCount()));
        record.put("orig_dns_rcode", Integer.valueOf(dnsResponseHdr.getRawRcode()));
        record.put("orig_dns_opcode", Integer.valueOf(dnsResponseHdr.getRawOpcode()));
        record.put("orig_dns_labels", Integer.valueOf(labels));

        if (q != null) {
          // unassinged, private or unknown, get raw value
          record.put("orig_dns_qtype", Integer.valueOf(q.getQTypeValue()));
          // unassinged, private or unknown, get raw value)
          record.put("orig_dns_qclass", Integer.valueOf(q.getQClassValue()));
        }

        OPTResourceRecord opt = dnsResponseMessage.getPseudo();
        if (opt != null) {

          record.put("orig_dns_edns_udp", Integer.valueOf(opt.getUdpPlayloadSize()));
          record.put("orig_dns_edns_version", Integer.valueOf(opt.getVersion()));
          record.put("orig_dns_edns_do", Boolean.valueOf(opt.isDnssecDo()));
        }
      }
    }

    return Pair.create(record, mv);
  }

  @Override
  public ProtocolType type() {
    return ProtocolType.ICMP;
  }


}

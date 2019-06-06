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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import nl.sidnlabs.dnslib.message.Header;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.message.Question;
import nl.sidnlabs.dnslib.message.records.edns0.OPTResourceRecord;
import nl.sidnlabs.dnslib.util.Domaininfo;
import nl.sidnlabs.dnslib.util.NameUtil;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.ICMPPacket;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.entrada.config.Settings;
import nl.sidnlabs.entrada.ip.geo.GeoIPService;
import nl.sidnlabs.entrada.metric.MetricManager;
import nl.sidnlabs.entrada.support.PacketCombination;

@Component
public class ICMPParquetPacketWriter extends AbstractParquetPacketWriter {

  private static final String ICMP_AVRO_SCHEMA = "icmp-packet.avsc";

  // stats counters
  private int v4;
  private int v6;
  private int typeError;
  private int typeInfo;
  private Map<Integer, Integer> typesV4 = new HashMap<>();
  private Map<Integer, Integer> typesV6 = new HashMap<>();

  private Settings settings;
  private MetricManager metricManager;

  public ICMPParquetPacketWriter(Settings settings, MetricManager metricManager,
      GeoIPService geoLookup) {
    super(geoLookup);
    this.settings = settings;
    this.metricManager = metricManager;
  }

  @Override
  public void write(PacketCombination packetCombo) {

    GenericRecordBuilder builder = recordBuilder(ICMP_AVRO_SCHEMA);

    ICMPPacket icmpPacket = (ICMPPacket) packetCombo.getRequest();

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

    // icmp packet ip+headers
    Timestamp packetTime = new Timestamp((icmpPacket.getTs() * 1000));
    Optional<String> country = getCountry(icmpPacket.getSrc());
    Optional<String> asn = getAsn(icmpPacket.getSrc());

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
      domaininfo = NameUtil.getDomain(normalizedQname, settings.getTldSuffixes());
    }

    // values from query now.
    builder
        .set("svr", packetCombo.getServer().getName())
        .set("unixtime", icmpPacket.getTs())
        .set("time_micro", icmpPacket.getTsmicros())
        .set("time", packetTime.getTime())
        .set("icmp_type", icmpPacket.getType())
        .set("icmp_code", icmpPacket.getCode())
        .set("icmp_echo_client_type", icmpPacket.getClientType())
        .set("ip_ttl", icmpPacket.getTtl())
        .set("ip_v", (int) icmpPacket.getIpVersion())
        .set("ip_src", icmpPacket.getSrc())
        .set("ip_dst", icmpPacket.getDst())
        .set("ip_country", country.orElse(null))
        .set("ip_asn", asn.orElse(null))
        .set("l4_prot", (int) icmpPacket.getProtocol())
        .set("l4_srcp", icmpPacket.getSrcPort())
        .set("l4_dstp", icmpPacket.getDstPort())
        .set("ip_len", icmpPacket.getTotalLength()); // size
                                                     // of
                                                     // ip
                                                     // packet
                                                     // incl
                                                     // headers

    // add file name
    builder.set("pcap_file", packetCombo.getPcapFilename());

    // if no anycast location is encoded in the name then the anycast server name and location will
    // be null
    // only store this column in case of anycast, to save storage space.
    // the server name can be determined with the "svr" column
    builder.set("server_location", packetCombo.getServer().getLocation());

    // orig packet from payload

    if (originalPacket != null && originalPacket != Packet.NULL) {

      builder
          .set("orig_ip_ttl", originalPacket.getTtl())
          .set("orig_ip_v", (int) originalPacket.getIpVersion())
          .set("orig_ip_src", originalPacket.getSrc())
          .set("orig_ip_dst", originalPacket.getDst())
          .set("orig_l4_prot", (int) originalPacket.getProtocol())
          .set("orig_l4_srcp", originalPacket.getSrcPort())
          .set("orig_l4_dstp", originalPacket.getDstPort())
          .set("orig_udp_sum", originalPacket.getUdpsum())
          .set("orig_ip_len", originalPacket.getTotalLength()); // size of ip packet incl headers

      if (originalICMPPacket != null) {
        builder
            .set("orig_icmp_type", originalICMPPacket.getType())
            .set("orig_icmp_code", originalICMPPacket.getCode())
            .set("orig_icmp_echo_client_type", originalICMPPacket.getClientType());
      }

      if (dnsResponseMessage != null) {
        // orig dns response from icmp packet
        builder
            .set("orig_dns_len", originalPacket.getPayloadLength()) // get the size from the
                                                                    // reassembled udp header of
                                                                    // the original udp response
            .set("orig_dns_id", dnsResponseHdr.getId())
            .set("orig_dns_qname", normalizedQname)
            .set("orig_dns_domainname", domaininfo.getName())
            .set("orig_dns_aa", dnsResponseHdr.isAa())
            .set("orig_dns_tc", dnsResponseHdr.isTc())
            .set("orig_dns_rd", dnsResponseHdr.isRd())
            .set("orig_dns_ra", dnsResponseHdr.isRa())
            .set("orig_dns_z", dnsResponseHdr.isZ())
            .set("orig_dns_ad", dnsResponseHdr.isAd())
            .set("orig_dns_cd", dnsResponseHdr.isCd())
            .set("orig_dns_ancount", (int) dnsResponseHdr.getAnCount())
            .set("orig_dns_arcount", (int) dnsResponseHdr.getArCount())
            .set("orig_dns_nscount", (int) dnsResponseHdr.getNsCount())
            .set("orig_dns_qdcount", (int) dnsResponseHdr.getQdCount())
            .set("orig_dns_rcode", dnsResponseHdr.getRawRcode())
            .set("orig_dns_opcode", dnsResponseHdr.getRawOpcode())
            // set edns0 defaults
            .set("orig_dns_edns_udp", null)
            .set("orig_dns_edns_version", null)
            .set("orig_dns_edns_do", null)
            .set("orig_dns_labels", domaininfo.getLabels());

        if (q != null) {
          // unassinged, private or unknown, get raw value
          builder.set("orig_dns_qtype", q.getQTypeValue());
          // unassinged, private or unknown, get raw value
          builder.set("orig_dns_qclass", q.getQClassValue());
        }

        OPTResourceRecord opt = dnsResponseMessage.getPseudo();
        if (opt != null) {
          builder
              .set("orig_dns_edns_udp", (int) opt.getUdpPlayloadSize())
              .set("orig_dns_edns_version", (int) opt.getVersion())
              .set("orig_dns_edns_do", opt.isDnssecDo());
        }
      }
    }

    GenericRecord record = builder.build();
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(icmpPacket.getTs());

    writer
        .write(record, schema(ICMP_AVRO_SCHEMA), cal.get(Calendar.YEAR), cal.get(Calendar.MONTH),
            cal.get(Calendar.DAY_OF_MONTH));

    // write stats
    if (icmpPacket.isIPv4()) {
      v4++;
      updateMetricMap(typesV4, Integer.valueOf((icmpPacket.getType())));
    } else {
      v6++;
      updateMetricMap(typesV6, Integer.valueOf(icmpPacket.getType()));
    }

    if (icmpPacket.isError()) {
      typeError++;
    } else {
      typeInfo++;
    }
  }

  @Override
  public void writeMetrics() {
    metricManager.send(MetricManager.METRIC_ICMP_V4, v4);
    metricManager.send(MetricManager.METRIC_ICMP_V4, v6);
    writeMetrics(metricManager, typesV4, MetricManager.METRIC_ICMP_PREFIX_TYPE_V4);
    writeMetrics(metricManager, typesV6, MetricManager.METRIC_ICMP_PREFIX_TYPE_V6);
    metricManager.send(MetricManager.METRIC_ICMP_ERROR, typeError);
    metricManager.send(MetricManager.METRIC_ICMP_INFO, typeInfo);
  }

  protected void writeMetrics(MetricManager mm, Map<Integer, Integer> map, String prefix) {
    map
        .entrySet()
        .stream()
        .forEach(entry -> mm
            .send(prefix + "." + entry.getValue() + ".count", entry.getValue().intValue()));
  }


}


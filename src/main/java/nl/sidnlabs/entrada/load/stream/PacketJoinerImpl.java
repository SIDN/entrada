package nl.sidnlabs.entrada.load.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.types.MessageType;
import nl.sidnlabs.dnslib.types.ResourceRecordType;
import nl.sidnlabs.entrada.load.PacketJoiner;
import nl.sidnlabs.entrada.support.RequestCacheKey;
import nl.sidnlabs.entrada.support.RequestCacheValue;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.pcap.packet.DNSPacket;
import nl.sidnlabs.pcap.packet.Packet;
import nl.sidnlabs.pcap.packet.PacketFactory;

@Log4j2
@Component
@Getter
// use prototype scope, create new bean each time batch of files is processed
// this to avoid problems with memory/caches when running app for a long period of time
// and having cached data for multiple servers in the same bean instance
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PacketJoinerImpl implements PacketJoiner {

  @Value("${entrada.icmp.enable}")
  private boolean icmpEnabled;
  @Value("${entrada.cache.timeout:2}")
  private int cacheTimeoutConfig;

  private Map<RequestCacheKey, RequestCacheValue> requestCache = new HashMap<>();
  // keep list of active zone transfers
  private Map<RequestCacheKey, Integer> activeZoneTransfers = new HashMap<>();

  private long lastPacketTs = 0;
  private int cacheTimeout;
  // stats counters
  private int counter = 0;
  private int requestPacketCounter = 0;
  private int responsePacketCounter = 0;

  @PostConstruct
  private void init() {
    this.cacheTimeout = cacheTimeoutConfig * 1000;
  }

  public List<RowData> join(Packet p) {

    if (p == Packet.NULL) {
      // ignore, but do purge first
      return Collections.emptyList();
    }

    if (p == Packet.LAST) {
      // ignore, but do purge first
      return purge();
    }

    counter++;

    if (counter % 100000 == 0) {
      log.info("Received {} packets to join", counter);
    }

    if (isICMP(p)) {
      if (!icmpEnabled) {
        // do not process ICMP packets
        return Collections.emptyList();
      }
      // handle icmp
      List<RowData> results = new ArrayList<>(1);
      results.add(new RowData(p, null, null, null, false, p.getFilename()));
      return results;

    } else {
      // must be dnspacket
      DNSPacket dnsPacket = (DNSPacket) p;

      if (dnsPacket.getMessages().isEmpty()) {
        // skip malformed packets
        log.debug("Packet contains no dns message, skipping...");
        return Collections.emptyList();
      }

      lastPacketTs = p.getTsMilli();

      List<RowData> results = new ArrayList<>();
      for (Message msg : dnsPacket.getMessages()) {
        // put request into map until we find matching response, with a key based on: query id,
        // qname, ip src, tcp/udp port add time for possible timeout eviction
        if (msg.getHeader().getQr() == MessageType.QUERY) {
          handDnsRequest(dnsPacket, msg, p.getFilename());
        } else {
          RowData d = handDnsResponse(dnsPacket, msg, p.getFilename());
          if (d != null) {
            results.add(d);
          }
        }
      }
      // clear the packet which may contain many dns messages
      dnsPacket.clear();


      return results;
    } // end of dns packet
  }

  private boolean isICMP(Packet p) {
    return p.getProtocol() == PacketFactory.PROTOCOL_ICMP_V4
        || p.getProtocol() == PacketFactory.PROTOCOL_ICMP_V6;
  }

  private void handDnsRequest(DNSPacket dnsPacket, Message msg, String fileName) {
    requestPacketCounter++;
    // check for ixfr/axfr request
    if (!msg.getQuestions().isEmpty()
        && (msg.getQuestions().get(0).getQType() == ResourceRecordType.AXFR
            || msg.getQuestions().get(0).getQType() == ResourceRecordType.IXFR)) {

      if (log.isDebugEnabled()) {
        log.debug("Detected zone transfer for: " + dnsPacket.getFlow());
      }
      // keep track of ongoing zone transfer, we do not want to store all the response
      // packets for an ixfr/axfr.
      activeZoneTransfers
          .put(new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getSrc(),
              dnsPacket.getSrcPort(), 0), 0);
    }

    RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), qname(msg),
        dnsPacket.getSrc(), dnsPacket.getSrcPort(), dnsPacket.getTsMilli());

    if (log.isDebugEnabled()) {
      log.info("Insert into cache key: " + key);
    }

    // put the query in the cache until we get a matching response
    requestCache.put(key, new RequestCacheValue(msg, dnsPacket, fileName));
  }

  private RowData handDnsResponse(DNSPacket dnsPacket, Message msg, String fileName) {
    responsePacketCounter++;
    // try to find the request

    // check for ixfr/axfr response, the query might be missing from the response
    // so we cannot use the qname for matching.
    RequestCacheKey key = new RequestCacheKey(msg.getHeader().getId(), null, dnsPacket.getDst(),
        dnsPacket.getDstPort(), 0);
    if (activeZoneTransfers.containsKey(key)) {
      if (log.isDebugEnabled()) {
        log.debug("Ignore {} zone transfer response(s)", msg.getAnswer().size());
      }
      // this response is part of an active zonetransfer.
      // only let the first response continue, reuse the "time" field of the RequestKey to
      // keep track of this.
      Integer ztResponseCounter = activeZoneTransfers.get(key);
      if (ztResponseCounter.intValue() > 0) {
        // do not save this msg, drop it here, continue with next msg.
        return null;
      } else {
        // 1st response msg let it continue, add 1 to the map the indicate 1st resp msg
        // has been processed
        activeZoneTransfers.put(key, 1);
      }
    }
    String qname = qname(msg);

    key = new RequestCacheKey(msg.getHeader().getId(), qname, dnsPacket.getDst(),
        dnsPacket.getDstPort(), 0);

    if (log.isDebugEnabled()) {
      log.debug("Get from cache key: " + key);
      log.debug("request cache size before: " + requestCache.size());
    }

    RequestCacheValue request = requestCache.remove(key);
    // check to see if the request msg exists, at the start of the pcap there may be
    // missing queries
    if (log.isDebugEnabled()) {
      log.debug("request cache size after: " + requestCache.size());
    }

    if (request != null && request.getPacket() != null && request.getMessage() != null) {

      // pushRow(
      return new RowData(request.getPacket(), request.getMessage(), dnsPacket, msg, false,
          fileName);
      // );

    } else {
      // no request found, this could happen if the query was in previous pcap
      // and was not correctly decoded, or the request timed out before server
      // could send a response.

      if (log.isDebugEnabled()) {
        log.debug("Found no request for response, dst: " + dnsPacket.getDst() + " qname: " + qname);
      }

      if (qname != null) {
        // pushRow(
        return new RowData(null, null, dnsPacket, msg, false, fileName);
        // );
      }
    }

    return null;
  }

  /**
   * get qname from request which is part of the cache lookup key
   * 
   * @param msg the DNS message
   * @return the qname from the DNS question or null if not found.
   */
  private String qname(Message msg) {
    String qname = null;
    if (!msg.getQuestions().isEmpty()) {
      qname = msg.getQuestions().get(0).getQName();
    }

    return qname;
  }

  public void logStats() {
    log.info("-------------- Done processing pcap file -----------------");
    log.info("{} total DNS messages: ", counter);
    log.info("{} requests: ", requestPacketCounter);
    log.info("{} responses: ", responsePacketCounter);
    log.info("{} request cache size: ", requestCache.size());
  }

  public Map<RequestCacheKey, RequestCacheValue> getRequestCache() {
    return requestCache;
  }

  public void setRequestCache(Map<RequestCacheKey, RequestCacheValue> requestCache) {
    this.requestCache = requestCache;
  }


  private List<RowData> purge() {
    // remove expired entries from requestCache
    Iterator<RequestCacheKey> iter = requestCache.keySet().iterator();
    // use time from pcap to calc max age of cached packets
    long max = lastPacketTs - cacheTimeout;
    int purgeCounter = 0;
    int oldSize = requestCache.size();

    List<RowData> expired = new ArrayList<>();

    while (iter.hasNext()) {
      RequestCacheKey key = iter.next();
      // add the expiration time to the key and see if this leads to a time which is after the
      // current time.
      if (key.getTime() < max) {
        // remove expired request
        RequestCacheValue cacheValue = requestCache.get(key);
        iter.remove();

        if (cacheValue.getMessage() != null && !cacheValue.getMessage().getQuestions().isEmpty()
            && cacheValue.getMessage().getHeader().getQr() == MessageType.QUERY) {

          expired
              .add(new RowData(cacheValue.getPacket(), cacheValue.getMessage(), null, null, true,
                  cacheValue.getFilename()));


          if (log.isDebugEnabled()) {
            log
                .debug("Expired query for: "
                    + cacheValue.getMessage().getQuestions().get(0).getQName());
          }
          purgeCounter++;
        }
      }
    }

    log.info("-------------- Joiner Cache Purge Stats ------------------");
    log.info("Size before: {}", oldSize);
    log.info("Purge TTL: {}", cacheTimeout);
    log.info("Purge lastPacketTs: {}", lastPacketTs);
    log.info("Purge max: {}", max);
    log.info("Expired query's with rcode -1 (no response): {}", purgeCounter);
    log.info("Size after: {}", requestCache.size());

    return expired;
  }

  @Override
  public void reset() {
    counter = 0;
    requestPacketCounter = 0;
    responsePacketCounter = 0;
  }

}

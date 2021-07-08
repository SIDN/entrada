package nl.sidnlabs.entrada.load;

import java.util.List;
import java.util.Map;
import nl.sidnlabs.entrada.support.RequestCacheKey;
import nl.sidnlabs.entrada.support.RequestCacheValue;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.pcap.packet.Packet;


public interface PacketJoiner {

  Map<RequestCacheKey, RequestCacheValue> getRequestCache();

  void setRequestCache(Map<RequestCacheKey, RequestCacheValue> cache);

  void reset();

  List<RowData> join(Packet p);

  int getCounter();
}

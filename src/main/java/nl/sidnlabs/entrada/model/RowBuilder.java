package nl.sidnlabs.entrada.model;

import org.apache.avro.generic.GenericRecord;
import akka.japi.Pair;
import nl.sidnlabs.entrada.support.RowData;
import nl.sidnlabs.pcap.packet.Packet;

public interface RowBuilder {

  Pair<GenericRecord, BaseMetricValues> build(RowData combo, String server);

  Pair<GenericRecord, BaseMetricValues> build(Packet p, String server);

  void reset();

  void printStats();

  ProtocolType type();

}

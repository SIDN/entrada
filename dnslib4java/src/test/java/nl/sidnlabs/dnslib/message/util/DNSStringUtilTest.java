package nl.sidnlabs.dnslib.message.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.Test;
import nl.sidnlabs.dnslib.message.Message;
import nl.sidnlabs.dnslib.message.RRset;
import nl.sidnlabs.dnslib.message.records.AAAAResourceRecord;
import nl.sidnlabs.dnslib.message.records.AResourceRecord;
import nl.sidnlabs.dnslib.message.records.NSResourceRecord;
import nl.sidnlabs.dnslib.message.records.ResourceRecord;



public class DNSStringUtilTest {

  @Test
  public void readName() {
    byte[] bytes = new byte[] {-117, 117, -128, 0, 0, 1, 0, 0, 0, 7, 0, 1, 3, 119, 119, 119, 20,
        108, 101, 115, 100, 111, 117, 99, 101, 117, 114, 115, 100, 97, 109, 97, 110, 100, 105, 110,
        101, 2, 98, 101, 0, 0, 1, 0, 1, -64, 16, 0, 2, 0, 1, 0, 1, 81, -128, 0, 23, 4, 100, 110,
        115, 51, 13, 49, 50, 51, 104, 106, 101, 109, 109, 101, 115, 105, 100, 101, 2, 100, 107, 0,
        -64, 16, 0, 2, 0, 1, 0, 1, 81, -128, 0, 7, 4, 100, 110, 115, 50, -64, 62, -64, 16, 0, 2, 0,
        1, 0, 1, 81, -128, 0, 7, 4, 100, 110, 115, 49, -64, 62, 32, 98, 97, 49, 52, 49, 115, 110,
        114, 110, 111, 101, 49, 114, 99, 57, 109, 100, 100, 103, 114, 101, 115, 116, 50, 51, 103,
        54, 53, 55, 114, 105, 114, -64, 37, 0, 50, 0, 1, 0, 0, 2, 88, 0, 39, 1, 1, 0, 5, 4, 26, 78,
        -101, 108, 20, 90, -126, 114, -88, -42, 57, 101, -37, -11, -94, 110, 11, -71, 106, -84, -3,
        -120, 88, 114, -37, 0, 7, 34, 0, 0, 0, 0, 2, -112, -64, 118, 0, 46, 0, 1, 0, 0, 2, 88, 0,
        -106, 0, 50, 8, 2, 0, 0, 2, 88, 92, 115, -18, -4, 92, 102, -73, -106, 9, -82, 2, 98, 101, 0,
        120, 36, 32, 82, 100, -31, 42, -93, -60, -34, -41, 78, 4, 16, 119, 48, -36, 110, -56, -81,
        67, 114, -78, -23, -107, 28, 56, -107, 110, -5, 39, 13, 23, -101, -3, -95, 103, 75, 41, 3,
        -72, 111, -93, -26, -86, -26, -29, 29, 43, 4, 64, 19, -38, -55, -4, 57, 20, -121, -12, -90,
        -37, 80, -63, -68, -92, -120, 112, -82, -3, -50, -126, 64, -116, 36, 19, 27, 103, -101, 81,
        43, -60, -98, 68, -40, 77, -65, 82, -104, 47, 67, -83, -123, 2, 82, 7, 111, -126, -108, 69,
        -103, 81, -92, 81, -91, 69, -77, -41, -125, -65, 68, -32, 31, 23, -18, 69, 88, -91, -17,
        -15, -118, -12, 14, -58, 21, -62, 93, 82, 120, 32, 103, 113, 50, 115, 97, 53, 117, 101, 102,
        97, 113, 104, 103, 109, 56, 107, 117, 52, 53, 112, 112, 107, 98, 111, 49, 56, 111, 49, 48,
        99, 98, 114, -64, 37, 0, 50, 0, 1, 0, 0, 2, 88, 0, 38, 1, 1, 0, 5, 4, 26, 78, -101, 108, 20,
        -122, -123, -45, -47, 112, 114, -97, 123, -82, -48, -53, 31, -10, -87, -93, 22, -13, 53,
        -49, -88, 0, 6, 32, 0, 0, 0, 0, 18, -63, 108, 0, 46, 0, 1, 0, 0, 2, 88, 0, -106, 0, 50, 8,
        2, 0, 0, 2, 88, 92, 107, 117, -111, 92, 94, 61, -128, 9, -82, 2, 98, 101, 0, 91, -42, -87,
        -18, 62, 17, -128, -66, 117, -60, 113, -14, -89, 125, 12, 116, -32, -115, -77, 80, 73, 64,
        81, 36, 97, 123, 60, 105, 20, 78, 7, -112, 25, 38, 83, -127, 59, -31, 9, -7, -63, 46, -46,
        99, -31, -123, -8, -32, 45, -74, 47, 116, -93, 11, 15, 88, 2, 93, 9, 44, 15, 104, -118, 105,
        45, 78, -91, -123, 52, 37, -124, -66, 53, 4, -54, -51, -59, 4, 84, 108, 8, 28, 29, -82, -28,
        56, -27, -128, -23, 56, 10, -37, 30, 61, -36, -74, 28, -127, -9, 94, 4, -112, 98, -120,
        -128, 55, -5, -20, 80, 12, -59, 58, 126, -7, 49, -60, -106, -123, 85, -70, -9, 38, -89, 34,
        95, 28, -89, -34, 0, 0, 41, 16, 0, 0, 0, -128, 0, 0, 0};
    NetworkData buffer = new NetworkData(bytes);

    // go to a compressed name and try to read it
    buffer.setReaderIndex(92);
    String name = DNSStringUtil.readName(buffer);
    assertEquals("dns2.123hjemmeside.dk.", name);
  }

  @Test
  public void decodeDnsMessage() {
    byte[] bytes = new byte[] {111, -121, -128, 0, 0, 1, 0, 0, 0, 2, 0, 1, 3, 119, 119, 119, 12,
        101, 45, 103, 101, 122, 111, 110, 100, 104, 101, 105, 100, 2, 98, 101, 0, 0, 1, 0, 1, -64,
        16, 0, 2, 0, 1, 0, 1, 81, -128, 0, 25, 7, 103, 97, 110, 100, 97, 108, 102, 12, 115, 101,
        110, 105, 111, 114, 112, 108, 97, 110, 101, 116, 2, 102, 114, 0, -64, 16, 0, 2, 0, 1, 0, 1,
        81, -128, 0, 7, 4, 110, 115, 115, 112, -64, 57, 0, 0, 41, 16, 0, 0, 0, 0, 0, 0, 0};
    NetworkData networkData = new NetworkData(bytes);
    Message dnsMessage = new Message(networkData, false);
    assertEquals(1, dnsMessage.getAuthority().size());
    RRset rrSet = dnsMessage.getAuthority().get(0);
    assertEquals(2, rrSet.getAll().size());
    for (ResourceRecord record : rrSet.getAll()) {
      assertTrue(record instanceof NSResourceRecord);
    }
    NSResourceRecord ns1 = (NSResourceRecord) rrSet.getAll().get(0);
    NSResourceRecord ns2 = (NSResourceRecord) rrSet.getAll().get(1);

    assertEquals("gandalf.seniorplanet.fr.", ns1.getNameserver());
    assertEquals("nssp.seniorplanet.fr.", ns2.getNameserver());
  }

  @Test
  public void decodeQtypeNS() {
    byte[] data = bytes("pcap/sample_lookup_sidnlabs_nl_qtype_ns_response.bin");

    assertTrue(data.length > 0);

    // only read the dns data
    NetworkData networkData = new NetworkData(data);

    assertNotNull(networkData);

    Message dnsMessage = new Message(networkData, false);

    assertNotNull(dnsMessage);


    for (RRset rrset : dnsMessage.getAnswer()) {
      for (ResourceRecord record : rrset.getAll()) {
        assertTrue(record instanceof NSResourceRecord);

      }
    }

    assertEquals("ns2.sidn.nl.",
        ((NSResourceRecord) dnsMessage.getAnswer().get(0).getAll().get(0)).getNameserver());
    assertEquals("proteus.sidnlabs.nl.",
        ((NSResourceRecord) dnsMessage.getAnswer().get(0).getAll().get(1)).getNameserver());


    for (RRset rrset : dnsMessage.getAdditional()) {
      for (ResourceRecord record : rrset.getAll()) {
        assertTrue(record instanceof AResourceRecord || record instanceof AAAAResourceRecord);
      }
    }

    assertEquals("94.198.159.3",
        ((AResourceRecord) dnsMessage.getAdditional().get(0).getAll().get(0)).getAddress());
    assertEquals("2a00:d78:0:712:94:198:159:3",
        ((AAAAResourceRecord) dnsMessage.getAdditional().get(1).getAll().get(0)).getAddress());


  }


  @Test
  public void decodeQtypeA() {
    byte[] data = bytes("pcap/sample_lookup_sidnlabs_nl_qtype_a_response.bin");

    assertTrue(data.length > 0);

    // only read the dns data
    NetworkData networkData = new NetworkData(data);

    assertNotNull(networkData);

    Message dnsMessage = new Message(networkData, false);

    assertNotNull(dnsMessage);


    for (RRset rrset : dnsMessage.getAnswer()) {
      for (ResourceRecord record : rrset.getAll()) {
        assertTrue(record instanceof AResourceRecord || record instanceof AAAAResourceRecord);
      }
    }

    assertEquals("sidnlabs.nl.",
        ((AResourceRecord) dnsMessage.getAnswer().get(0).getAll().get(0)).getName());
    assertEquals("212.114.98.233",
        ((AResourceRecord) dnsMessage.getAnswer().get(0).getAll().get(0)).getAddress());

  }


  private byte[] bytes(String filename) {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(filename).getFile());
    try {
      return Files.readAllBytes(Paths.get(file.getAbsolutePath()));
    } catch (IOException e) {
      throw new RuntimeException("Cannot load data", e);
    }
  }

}

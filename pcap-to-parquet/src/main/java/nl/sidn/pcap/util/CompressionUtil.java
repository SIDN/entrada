package nl.sidn.pcap.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import nl.sidn.pcap.exception.ApplicationException;

public class CompressionUtil {

  private CompressionUtil() {}

  /**
   * wraps the inputstream with a decompressor based on a filename ending
   *
   * @param in The input stream to wrap with a decompressor
   * @param filename The filename from which we guess the correct decompressor
   * @return the compressor stream wrapped around the inputstream. If no decompressor is found,
   *         returns the inputstream as-is
   */
  public static InputStream getDecompressorStreamWrapper(InputStream in, String filename,
      int bufSize) throws IOException {
    String filenameLower = filename.toLowerCase();
    if (filenameLower.endsWith(".pcap")) {
      return in;
    } else if (filenameLower.endsWith(".gz")) {
      return new GZIPInputStream(in, bufSize);
    } else if (filenameLower.endsWith(".xz")) {
      return new XZCompressorInputStream(in);
    }

    // unkown file type
    throw new ApplicationException("Could not open file with unknown extension: " + filenameLower);
  }

}

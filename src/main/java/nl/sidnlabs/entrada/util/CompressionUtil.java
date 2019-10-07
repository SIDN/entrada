package nl.sidnlabs.entrada.util;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.StringUtils;
import org.tukaani.xz.XZInputStream;
import nl.sidnlabs.entrada.exception.ApplicationException;

public class CompressionUtil {

  private CompressionUtil() {}

  /**
   * wraps the inputstream with a decompressor based on a filename ending
   *
   * @param in The input stream to wrap with a decompressor
   * @param filename The filename from which we guess the correct decompressor
   * @param bufSize size of inputbuffer for GZIPInputStream only
   * @return the compressor stream wrapped around the inputstream. If no decompressor is found,
   *         returns the inputstream as-is
   * @throws IOException when stream cannot be created
   */
  public static InputStream getDecompressorStreamWrapper(InputStream in, String filename)
      throws IOException {

    if (StringUtils.endsWithIgnoreCase(filename, ".pcap")) {
      return in;
    } else if (StringUtils.endsWithIgnoreCase(filename, ".gz")) {
      return new GzipCompressorInputStream(in, true);
    } else if (StringUtils.endsWithIgnoreCase(filename, ".xz")) {
      return new XZInputStream(in);
    }

    // unkown file type
    throw new ApplicationException("Could not open file with unknown extension: " + filename);
  }

}

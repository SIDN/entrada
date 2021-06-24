package nl.sidnlabs.entrada.util;

public class StringUtil {

  private StringUtil() {}

  public static String[] splitFastIpv4(String value) {
    String[] r = new String[4];
    int pos = 0;
    for (int i = 0; i < 4; i++) {
      int idx = value.indexOf('.', pos);
      if (i < 3) {
        r[i] = value.substring(pos, idx);
      } else {
        r[i] = value.substring(pos);
      }
      pos = idx + 1;
      if (idx == -1 && i == 3) {
        return r;
      }
    }

    return null;
  }

}

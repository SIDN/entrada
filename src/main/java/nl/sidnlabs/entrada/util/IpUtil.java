package nl.sidnlabs.entrada.util;

public class IpUtil {

  private IpUtil() {}


  public static long ipToLong(String ipAddress) {

    String[] ipAddressInArray = StringUtil.splitFastIpv4(ipAddress);

    long result = 0;
    for (int i = 0; i < ipAddressInArray.length; i++) {

      int power = 3 - i;
      int ip = Integer.parseInt(ipAddressInArray[i]);
      result += ip * Math.pow(256, power);
    }

    return result;
  }


}

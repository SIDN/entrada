package nl.sidnlabs.entrada.util;

import java.util.Optional;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DownloadUtil {

  private DownloadUtil() {}

  public static Optional<byte[]> getAsBytes(String url, String logUrl, int timeout) {
    log.info("GET URL: " + logUrl);

    CloseableHttpClient client =
        HttpClientBuilder.create().setDefaultRequestConfig(createConfig(timeout * 1000)).useSystemProperties().build();
    try {
      HttpResponse response = client.execute(new HttpGet(url));

      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        return Optional.ofNullable(EntityUtils.toByteArray(response.getEntity()));
      }
    } catch (Exception e) {
      log.error("Errror executing HTTP GET request for: " + logUrl);
    }

    return Optional.empty();
  }

  private static RequestConfig createConfig(int timeoutMillis) {
    return RequestConfig
        .custom()
        // timeout for waiting during creating of connection
        .setConnectTimeout(timeoutMillis)
        .setConnectionRequestTimeout(timeoutMillis)
        .setSocketTimeout(timeoutMillis)
        // socket has timeout, for slow senders
        .setSocketTimeout(timeoutMillis)
        // do not let the apache http client initiate redirects
        .setRelativeRedirectsAllowed(false)
        .setRedirectsEnabled(false)
        .setCircularRedirectsAllowed(false)
        // build it
        .build();
  }


}

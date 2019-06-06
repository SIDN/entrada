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
package nl.sidnlabs.entrada.metric;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

/**
 * Adapter for sending metrics to the carbon database used by Graphite.
 *
 */
@Log4j2
@Component
public class GraphiteAdapter {

  @Value("${graphite.host}")
  private String host;

  @Value("${graphite.port}")
  private int port;

  @Value("${graphite.connect.timeout}")
  private int timeout;

  private Socket socket = null;
  private PrintWriter writer = null;

  public void connect() {
    try {
      socket = new Socket();
      socket.connect(new InetSocketAddress(host, port), timeout * 1000);
      writer = new PrintWriter(socket.getOutputStream(), true);
    } catch (Exception e) {
      log.error("Error connecting to Graphite", e);
      // ignore the error, it's ok to miss some metrics in graphite
      // missing some metrics if preferred instead of crashing the application
    }
  }

  public void send(String data) {
    if (writer != null && socket.isConnected()) {
      writer.print(data);
      writer.flush();
    }
  }

  public void close() {
    try {
      if (writer != null) {
        writer.close();
      }
      if (socket != null) {
        socket.close();
      }
    } catch (Exception e) {
      // just log
      log.error("Error closing connection to Graphite", e);
    }
  }

  public boolean isConnected() {
    return socket != null && !socket.isClosed() && socket.isConnected();
  }
}

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
package nl.sidn.pcap;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import lombok.extern.log4j.Log4j2;
import nl.sidn.metric.MetricManager;
import nl.sidn.pcap.config.Settings;
import nl.sidn.pcap.ip.GoogleResolverCheck;
import nl.sidn.pcap.ip.OpenDNSResolverCheck;
import nl.sidn.pcap.load.PcapProcessor;
import nl.sidn.pcap.util.FileUtil;

@Log4j2
@ComponentScan("nl.sidn")
@SpringBootApplication
public class Application implements CommandLineRunner {

  private Settings settings;
  private MetricManager metricManager;
  private GoogleResolverCheck googleResolverCheck;
  private OpenDNSResolverCheck openDNSResolverCheck;
  private PcapProcessor pcapProcessor;

  public Application(Settings settings, GoogleResolverCheck googleResolverCheck,
      OpenDNSResolverCheck openDNSResolverCheck, MetricManager metricManager,
      PcapProcessor pcapProcessor) {
    this.settings = settings;
    this.googleResolverCheck = googleResolverCheck;
    this.openDNSResolverCheck = openDNSResolverCheck;
    this.metricManager = metricManager;
    this.pcapProcessor = pcapProcessor;
  }

  public static void main(String[] args) {

    log.info("Started Spring container");

    SpringApplication.run(Application.class, args);

    log.info("Stopped Spring container");
  }


  @Override
  public void run(String... args) {
    long start = System.currentTimeMillis();
    debug(args);
    if (args.length != 3) {
      throw new IllegalArgumentException("Incorrect number of parameters found.");
    }

    settings.setServer(args[0]);
    settings.setInputDir(args[1]);
    settings.setOutputDir(args[2]);
    // settings.setStateDir(args[3]);

    // // path to config file
    // Settings.setPath(args[1]);
    // // name server name
    // Settings.getInstance().forServer(args[0]);
    // // set paths to input dir
    // Settings.getInstance().setSetting(Settings.INPUT_LOCATION, args[2]);
    // // set paths to output dir
    // Settings.getInstance().setSetting(Settings.OUTPUT_LOCATION, args[3]);
    // // set state location
    // Settings.getInstance().setSetting(Settings.STATE_LOCATION, args[4]);

    update();

    // do sanity check to see if files are present
    if (FileUtil.countFiles(settings.getInputDir() + System.getProperty("file.separator")
        + settings.getServerInfo().getFullname()) == 0) {
      log.info("No new PCAP files found, stop.");
      return;
    }

    // Controller controller = null;
    try {
      // controller = new Controller();
      // controller.start();

      pcapProcessor.execute();

    } catch (Exception e) {
      log.error("Error while loading data:", e);
      metricManager.send(MetricManager.METRIC_IMPORT_RUN_ERROR_COUNT, 1);
      // return non-zero status will allow script calling this program
      // stop further processing and goto abort path.
      System.exit(-1);
    } finally {
      // always send stats to monitoring
      long end = System.currentTimeMillis();
      int runtimeSecs = (int) (end - start) / 1000;
      metricManager.send(MetricManager.METRIC_IMPORT_RUN_TIME, runtimeSecs);
      metricManager.send(MetricManager.METRIC_IMPORT_RUN_ERROR_COUNT, 0);
      metricManager.flush();

      // if (controller != null) {
      // controller.close();
      // }
    }

    log.info("Done loading data");
  }


  private void debug(String[] args) {
    for (int i = 0; i < args.length; i++) {
      log.info("arg " + i + " = " + args[i]);
    }
  }

  private void update() {
    googleResolverCheck.update();
    openDNSResolverCheck.update();
  }



}

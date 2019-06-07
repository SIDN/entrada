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
package nl.sidnlabs.entrada;

import java.util.List;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.config.Settings;
import nl.sidnlabs.entrada.enrich.resolver.DnsResolverCheck;
import nl.sidnlabs.entrada.load.PcapProcessor;
import nl.sidnlabs.entrada.metric.MetricManager;
import nl.sidnlabs.entrada.util.FileUtil;

@Log4j2
@ComponentScan("nl.sidnlabs")
@SpringBootApplication
public class Application implements CommandLineRunner {

  private MetricManager metricManager;
  private PcapProcessor pcapProcessor;
  private List<DnsResolverCheck> resolverChecks;

  public Application(MetricManager metricManager, PcapProcessor pcapProcessor,
      List<DnsResolverCheck> resolverChecks) {

    this.metricManager = metricManager;
    this.pcapProcessor = pcapProcessor;
    this.resolverChecks = resolverChecks;
  }

  public static void main(String[] args) {

    debug(args);
    if (args.length != 4) {
      throw new IllegalArgumentException("Incorrect number of parameters found.");
    }
    // set the argument as static fields of Settings, cannot wait until Spring
    // creates a bean, because the constructor of Settings needs the arguments
    Settings.setServer(args[0]);
    Settings.setInputDir(args[1]);
    Settings.setOutputDir(args[2]);
    Settings.setStateDir(args[3]);

    log.info("Starting Spring container");

    SpringApplication.run(Application.class, args);

    log.info("Stopped Spring container");
  }


  @Override
  public void run(String... args) {
    long start = System.currentTimeMillis();
    // show resolver info to make sure the resolver data has been loaded
    resolverChecks.stream().forEach(r -> {
      r.init();
      log.info("Loaded {} IP subnets for {} resolver service", r.getSize(), r.getName());
    });

    // do sanity check to see if files are present
    if (FileUtil
        .countFiles(Settings.getInputDir() + System.getProperty("file.separator")
            + Settings.getServerInfo().getFullname()) == 0) {
      log.info("No new PCAP files found, stop.");
      return;
    }

    try {
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

    }

    log.info("Done loading data");

    // need to call exit otherwise H2 db will keep application alive
    System.exit(0);
  }


  private static void debug(String[] args) {
    log.info("Started application with following arguments:");
    for (int i = 0; i < args.length; i++) {
      log.info("argument " + i + " = " + args[i]);
    }
  }

}

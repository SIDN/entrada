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

import java.io.File;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class MainTest {

  @Autowired
  private Application main;

  @Test
  public void testRun() {

    ClassLoader classLoader = getClass().getClassLoader();

    File file = new File(classLoader.getResource("pcap/ns1.example.nl").getFile());

    String[] args =
        {"ns1.example.nl", classLoader.getResource("test-settings.properties").getFile(),
            file.getParent(), "/Users/maarten/sidn/development/tmp/pcap/parquet",
            "/Users/maarten/sidn/development/tmp/entrada/"};
    // main.run(args);
  }


  // @Test
  public void testRun2() {


    ClassLoader classLoader = getClass().getClassLoader();

    String[] args =
        {"ns3.dns.nl_syy", classLoader.getResource("test-settings.properties").getFile(),
            "/Users/maartenwullink/sidn/development/tmp/pcap/",
            "/Users/maartenwullink/sidn/development/tmp/pcap/parquet",
            "/Users/maartenwullink/sidn/development/tmp/entrada/"};
    main.run(args);
  }

}

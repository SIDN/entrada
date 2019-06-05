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
package nl.sidnlabs.pcap.util;

import java.io.File;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.pcap.exception.ApplicationException;

@Log4j2
public class FileUtil {

  private FileUtil() {}

  public static int countFiles(String inputDir) {
    log.info("Scan for pcap files in: " + inputDir);

    int filecount = 0;
    File f = FileUtils.getFile(inputDir);

    if (!f.exists()) {
      log
          .error("Directory {} does not exist, maybe this is due to a server name config error?",
              inputDir);
      return 0;
    }

    Iterator<File> files;
    try {
      files = FileUtils.iterateFiles(f, new String[] {"pcap", "pcap.gz", "pcap.xz"}, false);
      while (files.hasNext()) {
        files.next();
        filecount++;
      }
    } catch (Exception e) {
      throw new ApplicationException("Scanning for new files failed", e);
    }

    return filecount;
  }

}

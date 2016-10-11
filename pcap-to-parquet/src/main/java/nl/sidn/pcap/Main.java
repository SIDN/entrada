/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].
 *
 */	
package nl.sidn.pcap;

import nl.sidn.pcap.util.FileUtil;
import nl.sidn.pcap.util.Settings;
import nl.sidn.stats.MetricManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);


	public static void main(String[] args) {
		new Main().run(args);
	}
	
	public void run(String[] args){
		long start = System.currentTimeMillis();
		debug(args);
		if(args == null || args.length < 5){
			throw new RuntimeException("Incorrect number of parameters found.");
		}
		//path to config file
		Settings.setPath(args[1]);
		//name server name
		Settings.getInstance().forServer(args[0]);
		//set paths to input dir
		Settings.getInstance().setSetting(Settings.INPUT_LOCATION, args[2]);
		//set paths to output dir
		Settings.getInstance().setSetting(Settings.OUTPUT_LOCATION, args[3]);
		//set state location
		Settings.getInstance().setSetting(Settings.STATE_LOCATION, args[4]);
		
		//do sanity check to see if files are present
		if (FileUtil.countFiles(Settings.getInstance().getSetting(Settings.INPUT_LOCATION) + System.getProperty("file.separator") + Settings.getInstance().getServer().getFullname()) == 0 ){
			LOGGER.info("No new PCAP files found, stop.");
			return;
		}

		MetricManager mm = MetricManager.getInstance();
		Controller controller = null;
		try{
			controller = new Controller();
			controller.start();
		}catch(Exception e){
			LOGGER.error("Error while loading data:",e);
			mm.send(MetricManager.METRIC_IMPORT_RUN_ERROR_COUNT, 1);
			//return non-zero status will allow script calling this program
			//stop further processing and goto abort path.
			System.exit(-1);
		}
		finally{	
			//always send stats to monitoring
			long end = System.currentTimeMillis();
			int runtimeSecs = (int)(end-start)/1000;
			mm.send(MetricManager.METRIC_IMPORT_RUN_TIME, runtimeSecs);
			mm.send(MetricManager.METRIC_IMPORT_RUN_ERROR_COUNT, 0);
			mm.flush();
			controller.close();
		}
		
		LOGGER.info("Done loading data");
	}


	private void debug(String[] args){
		for (int i = 0; i < args.length; i++) {
			LOGGER.info("arg " + i + " = " + args[i]);
		}
	}

}

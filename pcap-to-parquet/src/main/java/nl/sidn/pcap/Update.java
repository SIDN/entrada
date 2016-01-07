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
import nl.sidn.pcap.ip.GoogleResolverCheck;
import nl.sidn.pcap.ip.OpenDNSResolverCheck;
import nl.sidn.pcap.util.Settings;


/**
 * Update the config data used by the pcap2parquet processing.
 *
 */
public class Update {

	public static void main(String[] args) {
		new Update().update(args);
	}
	
	public void update(String[] args){
		
		if(args == null || args.length < 2){
			throw new RuntimeException("Incorrect number of parameters found.");
		}
		
		Settings.setPath(args[0]);
		//set state location
		Settings.getInstance().setSetting(Settings.STATE_LOCATION, args[1]);
		//Google and OpenDNS checks will update when check object is created
		new GoogleResolverCheck().update();
		new OpenDNSResolverCheck().update();
	}
}

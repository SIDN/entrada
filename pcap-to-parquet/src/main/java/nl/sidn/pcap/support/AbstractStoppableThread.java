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
package nl.sidn.pcap.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractStoppableThread implements Runnable{
	
	public static final Log LOG = LogFactory.getLog(AbstractStoppableThread.class);

	private boolean keepRunning = true;
	
	@Override
	public void run() {
		try{
			doWork();
		}catch(Exception e){
			LOG.error("Thread threw exception", e);
			throw new RuntimeException(e);
		}
	}

	protected abstract void doWork();

	public void stop() {
		keepRunning = false;
	}
	
	public boolean isRunning(){
		return keepRunning;
	}
	
}

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
package nl.sidn.stats;

public class Metric implements Comparable<Metric> {
	
	private String name;
	private long value;
	private long time;
	private boolean useThreshHold;
	private boolean alive;
	
	private Metric(){}

	public Metric(String name, long value, long time) {
		this(name, value, time, true);
	}
	
	public Metric(String name, long value, long time, boolean useThreshHold) {
		this.name = name;
		this.value = value;
		this.time = time;
		this.useThreshHold = useThreshHold;
		this.alive = true;
	}

	public String getName() {
		return name;
	}

	public long getValue() {
		return value;
	}
	
	public void update(long value){
		this.value += value;
		this.alive = true;
	}

	public long getTime() {
		return time;
	}

	public boolean isUseThreshHold() {
		return useThreshHold;
	}

	/**
	 * @return pickle format, which can be sent to graphite.
	 */
	@Override
	public String toString() {
		return name + " " + value + " " + time;
	}

	public boolean isAlive() {
		return alive;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	@Override
	public int compareTo(Metric o) {
		return Integer.compare((int)this.time, (int)o.getTime());
	}

}

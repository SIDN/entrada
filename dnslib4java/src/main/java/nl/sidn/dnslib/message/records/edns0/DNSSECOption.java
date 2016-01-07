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
package nl.sidn.dnslib.message.records.edns0;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * @see http://tools.ietf.org/html/rfc6014
 *
 */
public class DNSSECOption extends EDNS0Option{
	
	public final static int OPTION_CODE_DAU = 5;
	public final static int OPTION_CODE_DHU = 6;
	public final static int OPTION_CODE_N3U = 7;
	
	private List<Integer> algs = new ArrayList<>();

	public DNSSECOption(int code, int len) {
		super(code, len);
	}
	
	public void addAlgorithm(int alg){
		algs.add(alg);
	}
	public List<Integer> getAlgorithms() {
		return algs;
	}

	@Override
	public String toString() {
		return "DNSSECOption [algs=" + algs + ", code=" + code + ", len=" + len
				+ "]";
	}
	
	public String export(){
		return StringUtils.join(algs, ',');
	}

}

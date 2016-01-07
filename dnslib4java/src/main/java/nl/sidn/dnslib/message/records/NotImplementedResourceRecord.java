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
package nl.sidn.dnslib.message.records;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import nl.sidn.dnslib.message.util.NetworkData;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class NotImplementedResourceRecord extends AbstractResourceRecord {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = Logger.getLogger(NotImplementedResourceRecord.class);

	@Override
	public void decode(NetworkData buffer) {
		super.decode(buffer);
		if(LOGGER.isDebugEnabled()){
			LOGGER.debug("decode unknown RR with name: " + getName());
			LOGGER.debug(" Unknown RR has size: " + (int)rdLength);
			LOGGER.debug(toZone(100));
		}
		
		if(rdLength > 0 ){
			buffer.setReaderIndex(buffer.getReaderIndex()+rdLength);
		}
	
	}

	@Override
	public void encode(NetworkData buffer) {
		super.encode(buffer);
		
		buffer.writeChar(rdLength);
		buffer.writeBytes(rdata);
		
	}
	
	@Override
	public String toZone(int maxLength) {
		StringBuffer b = new StringBuffer();
		int paddedSize = ( maxLength - name.length() ) + name.length();
		String ownerWithPadding = StringUtils.rightPad(name, paddedSize, " ");
		
		b.append(ownerWithPadding + "\t" + ttl + "\t" );
		
		if(classz == null){
			b.append("CLASS" + (int)rawClassz) ;
		}else{
			b.append(classz);
		}
		
		b.append("\t");
		if(type == null){
			b.append("TYPE" + (int)rawType);
		}else{
			b.append(type);
		}
		b.append("\t");
		
		b.append("\\# " + (int)rdLength);
		
		if(rdLength > 0){
			b.append(" " + Hex.encodeHexString(rdata));
		}
		
		
		return b.toString();
		
	}
	
	@Override
	public JsonObject toJSon(){
		String actualClass = null; 
		String actualType = null;
		if(classz == null){
			actualClass = "CLASS" + (int)rawClassz;
		}else{
			actualClass = classz.name();
		}
		
		if(type == null){
			actualType = "TYPE" + (int)rawType;
		}else{
			actualType = ""+type;
		}
		
		JsonObjectBuilder builder = Json.createObjectBuilder();
		return builder.
			add("rdata", Json.createObjectBuilder().
				add("class", actualClass).
				add("type", actualType).
				add("rdlength", (int)rdLength).
				add("rdata", Hex.encodeHexString(rdata))).
			build();
	}
	

}

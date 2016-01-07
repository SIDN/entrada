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
package nl.sidn.dnslib.logic;

import nl.sidn.dnslib.logic.unbound.UnboundLibrary;
import nl.sidn.dnslib.logic.unbound.ub_result;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.util.NetworkData;
import nl.sidn.dnslib.types.RcodeType;
import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;

import org.bridj.Pointer;

public class Resolver {
	
	private Pointer<UnboundLibrary.ub_ctx> ctx;
		
	public Resolver(){
		this(false);
	}
	
	public Resolver(boolean dnssec){
		ctx = UnboundLibrary.ub_ctx_create();
		//start with iterator only
		ResolverContextBuilder builder =  new ResolverContextBuilder();
		if(dnssec){
			ctx = builder.withDnsSecEnabled().build().getCtx();
		}else{
			ctx = builder.withIterator().build().getCtx();
		}
	}
	
	public Resolver(Context ctx){
		this.ctx = ctx.getCtx();
	}
	
	public LookupResult lookup(String qName, ResourceRecordType qType ){
		return lookup(qName, qType,ResourceRecordClass.IN,true);
	}

	public LookupResult lookup(String qName, ResourceRecordType qType, ResourceRecordClass qClazz, boolean decode ){

		Pointer<Byte> name = Pointer.pointerToCString(qName);

		Pointer<Pointer<ub_result>> result = Pointer.allocatePointer(ub_result.class);
		int status = UnboundLibrary.ub_resolve(ctx, name, qType.getValue(), qClazz.getValue(), result);
				
		//UnboundLibrary.ub_ctx_delete(ctx);
		 
		LookupResult lr = new LookupResult();
		ub_result ubr = result.get().get();
		lr.setRcode(RcodeType.fromValue(ubr.rcode()));		
		if(status == 0){
			lr.setOk(true);
			/* lookup was successful, get the result */
				
			lr.setHaveData(ubr.havedata() == 0? false: true);
			//dnssec result
			lr.setBogus(ubr.bogus() == 0? false: true);
			lr.setSecure(ubr.secure() == 0? false: true);
			if(lr.isBogus()){
				lr.setWhyBogus(ubr.why_bogus().getCString());
			}
			
			lr.setNxDomain(ubr.nxdomain() == 0? false: true);
			lr.setqName(ubr.qname().getCString());
			lr.setqType(ResourceRecordType.fromValue(ubr.qtype()));
			lr.setqClazz(ResourceRecordClass.fromValue(ubr.qclass()));		
			lr.setDatapacketLength(ubr.answer_len());
		    /* only get packetdata if any data is available */
			if(lr.getDatapacketLength() > 0){
				//set the raw bytes
				lr.setDatapacket(ubr.answer_packet().getBytes(lr.getDatapacketLength()));
				//if requested decode all the packet bytes
				if(decode){
					NetworkData nd = new NetworkData(lr.getDatapacket());
					Message msg = new Message();
					msg.decode(nd);
					lr.setPacket(msg);
				}
			}
			
			if(lr.isHaveData()){
				if(ubr.canonname() != null){
					lr.setCanconname(ubr.canonname().getCString());
				}
			}
			return lr;
		}else{
			lr.setOk(false);
			/* lookup failed, get the error message from libunbound */
			Pointer<Byte> errorString = UnboundLibrary.ub_strerror(status);
			errorString.getCString();
			if(errorString != null){
				lr.setStatus(errorString.getCString());
			}
			lr.setHaveData(false);
		}
		
		return lr;
		
	}
	
	public void cleanup(){
		if(ctx != null){
			UnboundLibrary.ub_ctx_delete(ctx);
			ctx = null;
		}
	}
}

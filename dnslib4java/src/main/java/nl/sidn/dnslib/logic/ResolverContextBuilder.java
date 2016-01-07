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
import nl.sidn.dnslib.trustanchor.TrustAnchor;
import nl.sidn.dnslib.trustanchor.TrustAnchorService;

import org.bridj.Pointer;

public class ResolverContextBuilder {
	
	private static TrustAnchor ta;
	private static String ds;
	
	static{
		init();
	}
	
	private Context ctx = new Context();
	
	private static synchronized void init() {
		if(ta == null){
			/* load root trust anchor */
			TrustAnchorService tas = new TrustAnchorService();
			ta = tas.loadTrustAnchor();
			//create ds record which is used by libunbound as ta for the root.
			ds = ". IN DS " + ta.getKeyDigest().getKeyTag() + " " + ta.getKeyDigest().getAlgorith() + " " + ta.getKeyDigest().getDigestType() + 
					" " + ta.getKeyDigest().getDigest();
		}
	}
	
	public ResolverContextBuilder withIterator() {
		addOption("module-config:", "iterator");
		return this;
	}
	
	public enum DEBUGMODE {OFF, MINIMAL, VERBOSE, EXTREME};
	
	public ResolverContextBuilder withDebug(DEBUGMODE mode) {
		if(mode == DEBUGMODE.OFF){
			UnboundLibrary.ub_ctx_debuglevel(ctx.getCtx(), 0);
		}else if(mode == DEBUGMODE.MINIMAL){
			UnboundLibrary.ub_ctx_debuglevel(ctx.getCtx(), 1);
		}else if(mode == DEBUGMODE.VERBOSE){
			UnboundLibrary.ub_ctx_debuglevel(ctx.getCtx(), 2);
		}else if(mode == DEBUGMODE.EXTREME){
			UnboundLibrary.ub_ctx_debuglevel(ctx.getCtx(), 3);
		}
		
		addOption("logfile:", "/var/log/libunbound.log");
		
		return this;
	}
	
	private void addOption(String option, String value){
		int status = UnboundLibrary.ub_ctx_set_option(ctx.getCtx(), Pointer.pointerToCString(option), Pointer.pointerToCString(value));
		if (status != 0) {
			throw new RuntimeException("Unable to add option to libunbound");
		}
	}
	
	public ResolverContextBuilder withDnsSecEnabled(){
		addOption("module-config:", "validator iterator");
		addOption("edns-buffer-size:", "4096");
		
		int status = UnboundLibrary.ub_ctx_add_ta(ctx.getCtx(), Pointer.pointerToCString(ds));
		if (status != 0) {
			throw new RuntimeException("Unable to add trust anchor to libunbound");
		}
		
		return this;
	}
	
	public ResolverContextBuilder withTCPEnabled(){
		addOption("tcp-upstream:", "yes");
		return this;
	}
	
	public ResolverContextBuilder withEdnsBufferSize(String size){
		addOption("edns-buffer-size:", size);
		return this;
	}
	
	public ResolverContextBuilder withOutboundPorts(String ports){
		addOption("outgoing-range:", ports);
		return this;
	}
	
	public ResolverContextBuilder withForwardingServer(String fwd){
		int status = UnboundLibrary.ub_ctx_set_fwd(ctx.getCtx(), Pointer.pointerToCString(fwd));
		if (status != 0) {
			throw new RuntimeException("Unable to set forwarding server for libunbound");
		}
		return this;
	}
	
	public Context build(){
		return ctx;
	}


}

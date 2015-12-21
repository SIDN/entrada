package nl.sidn.dnslib.logic;

import nl.sidn.dnslib.logic.unbound.UnboundLibrary;

import org.bridj.Pointer;

public class Context {
	
	private Pointer<UnboundLibrary.ub_ctx> ctx = UnboundLibrary.ub_ctx_create();

	public Pointer<UnboundLibrary.ub_ctx> getCtx() {
		return ctx;
	}
	
	

}

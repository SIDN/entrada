package nl.sidn.dnslib.logic;

import java.io.IOException;
import java.io.StringWriter;

import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Assert;
import org.junit.Test;

public class ResolverTest {
	
	@Test
	public void lookup(){
		Resolver r = new Resolver();
		LookupResult l = r.lookup("www.example.nl", ResourceRecordType.A, ResourceRecordClass.IN, true);
		Assert.assertNotNull(l);
		Assert.assertNotNull(l.getPacket());
	}
	
	@Test
	public void lookupDnsSecOk(){
		Resolver r = new Resolver(true);
		LookupResult l = r.lookup("www.sidn.nl", ResourceRecordType.A, ResourceRecordClass.IN, true);
		Assert.assertNotNull(l);
		Assert.assertNotNull(l.getPacket());
		Assert.assertTrue(l.isSecure());
		Assert.assertFalse(l.isBogus());
	}

	@Test
	public void lookupDnsSecBogus(){
		Resolver r = new Resolver(true);
		LookupResult l = r.lookup("www.servfail.nl", ResourceRecordType.A, ResourceRecordClass.IN, true);
		Assert.assertNotNull(l);
		Assert.assertNotNull(l.getPacket());
		Assert.assertFalse(l.isSecure());
		Assert.assertTrue(l.isBogus());
	}
	
	@Test
	public void lookupDnsJson()throws IOException{
		Resolver r = new Resolver(true);
		LookupResult l = r.lookup("www.sidn.nl", ResourceRecordType.A, ResourceRecordClass.IN, true);
		Assert.assertNotNull(l);
		Assert.assertNotNull(l.getPacket().toJson());
		System.out.println(l.getPacket().toJson());
	}
	
	@Test
	public void lookupDnsZone()throws IOException{
		Resolver r = new Resolver(true);
		LookupResult l = r.lookup("www.sidn.nl", ResourceRecordType.A, ResourceRecordClass.IN, true);
		Assert.assertNotNull(l);
		Assert.assertNotNull(l.getPacket().toZone());
		System.out.println(l.getPacket().toZone());
	}
}

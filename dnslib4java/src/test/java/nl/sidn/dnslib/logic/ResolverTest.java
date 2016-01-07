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

import java.io.IOException;

import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;

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

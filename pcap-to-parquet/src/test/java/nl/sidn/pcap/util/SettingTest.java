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
package nl.sidn.pcap.util;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import nl.sidn.dnslib.util.DomainParent;

public class SettingTest {

	@Before
	public void setup(){
		ClassLoader classLoader = getClass().getClassLoader();
		Settings.setPath(classLoader.getResource("test-settings.properties").getFile() );
	}
	@Test
	public void readSettings(){
		String val = Settings.getInstance().getSetting(Settings.RESOLVER_LIST_GOOGLE);
		Assert.assertEquals("https://developers.google.com/speed/public-dns/faq", val);
	}
	
	@Test
	public void createTldSuffix(){
		 Settings.createTldSuffixes("foo.test.co.example,foo.bar.entrada");
		 Assert.assertNotNull(Settings.getTldSuffixes());
		 Assert.assertTrue(Settings.getTldSuffixes().size() == 2);
		 
		 List<DomainParent> parents = Settings.getTldSuffixes();
		 DomainParent parent = parents.get(0);
		  
		 Assert.assertEquals(".foo.test.co.example", parent.getParent());
		 Assert.assertTrue(parent.getLabels() == 4);
		 
		 parent = parents.get(1);
		  
		 Assert.assertEquals(".foo.bar.entrada", parent.getParent());
		 Assert.assertTrue(parent.getLabels() == 3);
	}
}

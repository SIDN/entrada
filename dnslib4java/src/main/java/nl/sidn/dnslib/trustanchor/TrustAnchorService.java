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
package nl.sidn.dnslib.trustanchor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import nl.sidn.dnslib.util.FileDownloader;

import org.xml.sax.SAXException;

public class TrustAnchorService {
	
	private static String IANA_TRUST_ANCHOR_URL = "https://data.iana.org/root-anchors/root-anchors.xml";
	
	public TrustAnchor loadTrustAnchor(){
		FileDownloader fd = new FileDownloader();
		String data = fd.download(IANA_TRUST_ANCHOR_URL);
		return createTrustAnchor(data);
	}
		
	private TrustAnchor createTrustAnchor(String xml){
	
		try {
			SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI); 
			
			InputStream xsdStream = TrustAnchor.class.getClassLoader().getResourceAsStream("trust-anchor.xsd");
			if(xsdStream == null){
				xsdStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("trust-anchor.xsd");
			}
			StreamSource xsdSource = new StreamSource(xsdStream);
					
			Schema schema = sf.newSchema(xsdSource); 
 
			JAXBContext jc = JAXBContext.newInstance(TrustAnchor.class, KeyDigest.class);
 
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			unmarshaller.setSchema(schema);
			
			return (TrustAnchor) unmarshaller.unmarshal(new ByteArrayInputStream(xml.getBytes("UTF-8")));
		} catch (SAXException | JAXBException | UnsupportedEncodingException e) {
			throw new RuntimeException("Error while parsing trust anchor",e);
		}
		
	}


}

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

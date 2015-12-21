package nl.sidn.dnslib.message.util;

import java.io.StringWriter;
import java.util.Date;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import nl.sidn.dnslib.message.Header;
import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.message.RRset;
import nl.sidn.dnslib.message.records.ResourceRecord;
import nl.sidn.dnslib.message.records.edns0.OPTResourceRecord;

import org.apache.commons.codec.binary.Hex;

public class XMLTransformer {
	
	private XMLOutputFactory output = XMLOutputFactory.newInstance();
	private XMLStreamWriter writer ;
	private StringWriter sw ;
	
	public XMLTransformer(){
		sw = new StringWriter();
		try {
			writer = output.createXMLStreamWriter(sw);
		} catch (XMLStreamException e) {
			throw new RuntimeException("Error initializing XML transformer", e);
		}
	}
	
	public String transform(Message msg, long start,long duration){
		try {
			return doTransform(msg, start,duration);
		} catch (XMLStreamException e) {
			throw new RuntimeException("Error transforming data to XML", e);
		}
	}
	
	
	
	private String doTransform(Message msg, long start,long duration )throws XMLStreamException{
		List<RRset> rrsets = null;
		
		writer.writeStartDocument();
		writer.writeStartElement("response");
		
		writer.writeStartElement("start");	
		writer.writeCharacters(""+new Date(start));
		writer.writeEndElement();
		
		writer.writeStartElement("duration");	
		writer.writeCharacters(duration + "ms");
		writer.writeEndElement();
		
		transform(msg.getHeader());
			
		int count = (int)msg.getHeader().getAnCount();
		writer.writeStartElement("anscount");	
		writer.writeCharacters(""+count);
		writer.writeEndElement();
		if(count > 0){
			writer.writeStartElement("answers");
			rrsets = msg.getAnswer();	
			transform(rrsets, "answer");
			writer.writeEndElement();
		}
		
		count = (int)msg.getHeader().getNsCount();
		writer.writeStartElement("nscount");
		writer.writeCharacters(""+count);
		writer.writeEndElement();
		
		if(count > 0){
			writer.writeStartElement("authorities");
			rrsets = msg.getAuthority();	
			transform(rrsets, "authority");
			writer.writeEndElement();
		}

		count = (int)msg.getHeader().getArCount();
		writer.writeStartElement("arcount");
		writer.writeCharacters(""+count);
		writer.writeEndElement();
		
		if(count > 0){
			writer.writeStartElement("additionals");
			rrsets = msg.getAdditional();	
			transform(rrsets, "additional");
			writer.writeEndElement();
		}
		
		//end response
		writer.writeEndElement();
		writer.flush();
		
		return sw.toString();

	}
	
	private void transform(Header hdr)throws XMLStreamException{
		// aa:false, tc:false, rd:true, ra:true, ad:true, cd:false
		
		writer.writeStartElement("qr");	
		writer.writeCharacters(""+(int)hdr.getQr().getValue());
		writer.writeEndElement();
		
		writer.writeStartElement("opcode");	
		writer.writeCharacters(""+(int)hdr.getOpCode().getValue());
		writer.writeEndElement();
		
		writer.writeStartElement("aa");	
		writer.writeCharacters(hdr.isAa()?"1":"0");
		writer.writeEndElement();
		
		writer.writeStartElement("tc");	
		writer.writeCharacters(hdr.isTc()?"1":"0");
		writer.writeEndElement();
		
		writer.writeStartElement("rd");	
		writer.writeCharacters(hdr.isRd()?"1":"0");
		writer.writeEndElement();
		
		writer.writeStartElement("ra");	
		writer.writeCharacters(hdr.isRa()?"1":"0");
		writer.writeEndElement();
		
		writer.writeStartElement("ad");	
		writer.writeCharacters(hdr.isAd()?"1":"0");
		writer.writeEndElement();
		
		writer.writeStartElement("cd");	
		writer.writeCharacters(hdr.isCd()?"1":"0");
		writer.writeEndElement();
		
		writer.writeStartElement("rcode");	
		writer.writeCharacters(""+(int)hdr.getRcode().getValue());
		writer.writeEndElement();
		
	}
	
	private void transform(List<RRset> rrsets, String name)throws XMLStreamException{
		
		
		for (RRset rrset : rrsets) {
			for (ResourceRecord rr : rrset.getAll()) {
				
				if(rr instanceof OPTResourceRecord){
					//skip the opt pseude rr
					continue;
				}
				writer.writeStartElement(name);
				
				writer.writeStartElement("name");
				writer.writeCharacters(rr.getName());
				writer.writeEndElement();
				
				writer.writeStartElement("type");
				writer.writeCharacters(rr.getType().name());
				writer.writeEndElement();
				
				writer.writeStartElement("class");
				writer.writeCharacters(rr.getClassz().name());
				writer.writeEndElement();
				
				writer.writeStartElement("ttl");
				writer.writeCharacters(""+rr.getTtl());
				writer.writeEndElement();
				
				writer.writeStartElement("rdlength");
				writer.writeCharacters(""+(int)rr.getRdlength());
				writer.writeEndElement();
				
				writer.writeStartElement("rdata");
				writer.writeCharacters(Hex.encodeHexString(rr.getRdata()));
				writer.writeEndElement();
				
				writer.writeEndElement();
				
			}
		}
		
	}

}

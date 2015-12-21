package nl.sidn.dnslib.message.records;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import nl.sidn.dnslib.message.util.DNSStringUtil;
import nl.sidn.dnslib.message.util.NetworkData;

public class SRVResourceRecord extends AbstractResourceRecord {
	
	private static final long serialVersionUID = 1L;
	
	private char priority;
	private char weight;
	private char port;
	private String target;


	@Override
	public void decode(NetworkData buffer) {
		super.decode(buffer);
	
		priority = buffer.readUnsignedChar();
		
		weight = buffer.readUnsignedChar();
		
		port = buffer.readUnsignedChar();
		
		target = DNSStringUtil.readName(buffer);
	}

	@Override
	public void encode(NetworkData buffer) {
		super.encode(buffer);
		
		buffer.writeChar(6 + target.length() + 2);
		
		buffer.writeChar(priority);
		
		buffer.writeChar(weight);
		
		buffer.writeChar(port);

		DNSStringUtil.writeName(target, buffer);
	}
	
	public String getCacheId(){
		return null;
	}

	@Override
	public String toString() {
		return "SRVResourceRecord [" + super.toString() + ", priority=" + (int)priority + ", weight=" + (int)weight
				+ ", port=" + (int)port + ", target=" + target + "]";
	}


	@Override
	public String toZone(int maxLength) {
		return super.toZone(maxLength) + "\t" + (int)priority + " " + (int)weight
				+ " " + (int)port + " " + target;
	}


	@Override
	public JsonObject toJSon(){
		JsonObjectBuilder builder = super.createJsonBuilder();
		return builder.
			add("rdata", Json.createObjectBuilder().
				add("priority", (int)priority)).
				add("weight", (int)weight).
				add("port", (int)port).
				add("target", target).
			build();
	}

}

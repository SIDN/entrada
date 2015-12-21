package nl.sidn.dnslib.message.records;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import nl.sidn.dnslib.message.util.DNSStringUtil;
import nl.sidn.dnslib.message.util.NetworkData;

public class PTRResourceRecord extends AbstractResourceRecord {
	
	private static final long serialVersionUID = 1L;
	
	private String ptrDname;
	

	@Override
	public void decode(NetworkData buffer) {
		super.decode(buffer);
		
		ptrDname = DNSStringUtil.readName(buffer);
	}

	@Override
	public void encode(NetworkData buffer) {
		super.encode(buffer);
		

		buffer.writeChar(ptrDname.length() + 2); 
		
		DNSStringUtil.writeName(ptrDname, buffer);
		
	}
	
	public String getCacheId(){
		return null;
	}

	@Override
	public String toString() {
		return "PTRResourceRecord [ptrDname=" + ptrDname + "]";
	}
	

	@Override
	public String toZone(int maxLength) {
		return super.toZone(maxLength) + "\t" + ptrDname;
	}
	
	@Override
	public JsonObject toJSon(){
		JsonObjectBuilder builder = super.createJsonBuilder();
		return builder.
			add("rdata", Json.createObjectBuilder().
				add("ptrdname", ptrDname)).
			build();
	}
	

}

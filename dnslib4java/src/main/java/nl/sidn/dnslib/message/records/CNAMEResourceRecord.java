package nl.sidn.dnslib.message.records;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import nl.sidn.dnslib.message.util.DNSStringUtil;
import nl.sidn.dnslib.message.util.NetworkData;

public class CNAMEResourceRecord extends AbstractResourceRecord {
	
	private static final long serialVersionUID = 1L;
	
	private String cname;
	
	@Override
	public void decode(NetworkData buffer) {
		super.decode(buffer);
	
		cname = DNSStringUtil.readName(buffer);
	}

	@Override
	public void encode(NetworkData buffer) {
		super.encode(buffer);
		
		//write rdlength
		buffer.writeChar(cname.length()+2);
		
		DNSStringUtil.writeName(cname, buffer);
		
	}
	
	public String getCacheId(){
		return null;
	}

	public String getCname() {
		return cname;
	}

	@Override
	public String toString() {
		return "CNAMEResourceRecord [cname=" + cname + "]";
	}
	

	@Override
	public String toZone(int maxLength) {
		return super.toZone(maxLength) + "\t" + cname;
	}
	
	@Override
	public JsonObject toJSon(){
		JsonObjectBuilder builder = super.createJsonBuilder();
		return builder.
			add("rdata", Json.createObjectBuilder().
				add("cname", cname)).
			build();
	}
	

}

package nl.sidn.dnslib.message.records;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import nl.sidn.dnslib.message.util.DNSStringUtil;
import nl.sidn.dnslib.message.util.NetworkData;

public class NSResourceRecord extends AbstractResourceRecord {
	
	private static final long serialVersionUID = 1L;
	
	private String nameserver;
	

	public String getNameserver() {
		return nameserver;
	}

	public void setNameserver(String nameserver) {
		this.nameserver = nameserver;
	}

	@Override
	public void decode(NetworkData buffer) {
		super.decode(buffer);
		
		setNameserver(DNSStringUtil.readName(buffer));
		
	}

	@Override
	public void encode(NetworkData buffer) {
		super.encode(buffer);
		
		//write rdlength
		buffer.writeChar(nameserver.length() + 2); //+ 2 for leading lenght byte + the terminating root
		
		DNSStringUtil.writeName(nameserver, buffer);
		
	}
	
	public String getCacheId(){
		return nameserver;
	}
	

	@Override
	public String toString() {
		return super.toString() + " NSResourceRecord [nameserver=" + nameserver + "]";
	}

	
	@Override
	public String toZone(int maxLength) {
		return super.toZone(maxLength) + "\t" + nameserver;
	}
	
	@Override
	public JsonObject toJSon(){
		JsonObjectBuilder builder = super.createJsonBuilder();
		return builder.
			add("rdata", Json.createObjectBuilder().
				add("nameserver", nameserver)).
			build();
	}
	


}

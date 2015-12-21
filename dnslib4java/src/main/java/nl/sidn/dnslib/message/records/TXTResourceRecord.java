package nl.sidn.dnslib.message.records;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import nl.sidn.dnslib.message.util.NetworkData;

public class TXTResourceRecord extends AbstractResourceRecord {
	
	private static final long serialVersionUID = 1L;
	
	protected String value = "";
	protected byte[] data;


	@Override
	public void decode(NetworkData buffer) {
		super.decode(buffer);
		//the txt rdata contains <length byte><string bytes>
		int bytesRead = 0;
		
		while(bytesRead < rdLength){
			int stringLength = buffer.readUnsignedByte();
			data = new byte[stringLength];
			buffer.readBytes(data);		
			value = value + new String(data);
			bytesRead = bytesRead + stringLength + 1;
		}
	}

	@Override
	public void encode(NetworkData buffer) {
		super.encode(buffer);

		//write rdlength
		buffer.writeChar(rdLength);
		buffer.writeByte(data.length);
		buffer.writeBytes(data);
		
	}
	
	public String getCacheId(){
		return null;
	}

	@Override
	public String toString() {
		return "TXTResourceRecord [value=" + value + "]";
	}


	@Override
	public String toZone(int maxLength) {
		return super.toZone(maxLength) + "\t" + value;
	}
	
	@Override
	public JsonObject toJSon(){
		JsonObjectBuilder builder = super.createJsonBuilder();
		return builder.
			add("rdata", Json.createObjectBuilder().
				add("txt-data", value)).
			build();
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}


}

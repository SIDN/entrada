package nl.sidn.dnslib.message.records;

import javax.json.JsonObject;

import nl.sidn.dnslib.message.util.NetworkData;
import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;

public interface ResourceRecord{

	String getName();

	void setName(String name);

	ResourceRecordType getType();

	void setType(ResourceRecordType type);

	ResourceRecordClass getClassz();

	void setClassz(ResourceRecordClass classz);

	long getTtl();

	void setTtl(long ttl);
	
	char getRdlength();
	
	byte[] getRdata();
	
	void decode(NetworkData buffer);
	
	void encode(NetworkData buffer);

	String toZone(int maxLength);
	
	JsonObject toJSon();

}

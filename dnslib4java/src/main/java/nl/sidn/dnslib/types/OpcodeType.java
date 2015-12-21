package nl.sidn.dnslib.types;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 * DNS OpCodes

	Registration Procedures
	Standards Action as modified by [RFC4020]
	Reference
	[RFC-ietf-dnsext-rfc6195bis-05][RFC1035]
	OpCode 	Name 	Reference 
	0	Query	[RFC1035]
	1	IQuery (Inverse Query, OBSOLETE)	[RFC3425]
	2	Status	[RFC1035]
	3	Unassigned	
	4	Notify	[RFC1996]
	5	Update	[RFC2136]
	6-15	Unassigned	

 *
 */
public enum OpcodeType {
	
	STANDARD(0),
	INVERSE(1),
	STATUS(2),
	UNASSIGNED_3(3),
	NOTIFY(4),
	UPPDATE(5),
	UNASSIGNED(-1);
	
	private int value;
	
	private static Map<String, OpcodeType> types = new HashMap<>();
	private static Map<Integer, OpcodeType> typesToInt = new HashMap<>();
	
	static{
		OpcodeType[] values = values();
		for (OpcodeType type : values) {
			types.put(type.name(), type);
			typesToInt.put(new Integer(type.getValue()), type);
		}
	}
	
	public static OpcodeType fromString(String name){
		return types.get(StringUtils.upperCase(name));
	}
	
	public static OpcodeType fromValue(int value){
		OpcodeType type = typesToInt.get(new Integer(value));
		if(type == null){
			return UNASSIGNED;
		}
		
		return type;
	}
	
	private OpcodeType(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	
}

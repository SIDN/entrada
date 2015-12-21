package nl.sidn.dnslib.types;

import java.util.HashMap;
import java.util.Map;

/**
 * Digest Algorithms

	Registration Procedures
	Standards Action
	Reference
	[RFC3658][RFC4034][RFC4035]
	Value 	Description 	Status 	Reference 
	0	Reserved	-	[RFC3658]
	1	SHA-1	MANDATORY	[RFC3658]
	2	SHA-256	MANDATORY	[RFC4509]
	3	GOST R 34.11-94	OPTIONAL	[RFC5933]
	4	SHA-384	OPTIONAL	[RFC6605]
	5-255	Unassigned	-	

 *
 */
public enum DigestType {
	
	RESERVED((byte)0, null),
	SHA1((byte)1, "SHA-1"),
	SHA256((byte)2,"SHA-256");
	
	private byte value;
	private String name;
	
	private DigestType(byte value, String name){
		this.value = value;
		this.name = name;
	}
	
	private static Map<Integer, DigestType> types = new HashMap<>();
	
	static{
		DigestType[] values = values();
		for (DigestType type : values) {
			types.put(new Integer(type.getValue()), type);
		}
	}

	public byte getValue() {
		return value;
	}
	
	public static DigestType fromValue(short value){
		return types.get(new Integer(value));
	}

	public String getName() {
		return name;
	}
	

}

package nl.sidn.dnslib.types;

public enum MessageType {
	
	QUERY(0),
	RESPONSE(1);
	
	private int value;

	private MessageType(int value) {
		this.value = value;
	}

	public static MessageType fromByte(byte type){
		if(0 == type){
			return QUERY;
		}
		
		return RESPONSE;
	}

	public int getValue() {
		return value;
	}

}

package nl.sidn.dnslib.types;

public class TypeMap {
	
	private ResourceRecordType type;
	private char value;
	
	public TypeMap(ResourceRecordType type, char value) {
		this.type = type;
		this.value = value;
	}

	public ResourceRecordType getType() {
		return type;
	}

	public char getValue() {
		return value;
	}
	
	public String name(){
		if(type == ResourceRecordType.RESERVED){
			return "TYPE"+(int)value;	
		}
		
		return type.name();
	}
	

}

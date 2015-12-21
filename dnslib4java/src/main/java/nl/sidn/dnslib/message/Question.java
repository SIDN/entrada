package nl.sidn.dnslib.message;

import javax.json.Json;
import javax.json.JsonObject;

import nl.sidn.dnslib.message.util.DNSStringUtil;
import nl.sidn.dnslib.message.util.NetworkData;
import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;

public class Question {
	
	private String qName;
	private ResourceRecordType qType;
	private int qTypeValue;
	private ResourceRecordClass qClass;
	private int qClassValue;
	
	public Question(){};
	
	public Question(String qName, ResourceRecordType qType,	ResourceRecordClass qClass) {
		this.qName = qName;
		this.qType = qType;
		this.qClass = qClass;
	}
	public String getqName() {
		return qName;
	}
	public void setqName(String qName) {
		this.qName = qName;
	}
	public ResourceRecordType getqType() {
		return qType;
	}
	public void setqType(ResourceRecordType qType) {
		this.qType = qType;
	}
	public ResourceRecordClass getqClass() {
		return qClass;
	}
	public void setqClass(ResourceRecordClass qClass) {
		this.qClass = qClass;
	}

	public void decode(NetworkData buffer) {
	
		String qname = DNSStringUtil.readName(buffer);
		//prevent NPs by setting qname to empty string
		setqName(qname != null? qname: "");
		
		qTypeValue = buffer.readUnsignedChar();
		setqType(ResourceRecordType.fromValue(qTypeValue));
		
		qClassValue = buffer.readUnsignedChar();
		setqClass(ResourceRecordClass.fromValue(qClassValue));
		
	}


	public int getqTypeValue() {
		return qTypeValue;
	}

	public int getqClassValue() {
		return qClassValue;
	}

	@Override
	public String toString() {
		return "Question [qName=" + qName + ", qType=" + qType + ", qClass="
				+ qClass + "]";
	}
	
	public JsonObject toJSon(){
		return Json.createObjectBuilder().
			add("qName", qName).
			add("qType", qType != null?qType.name(): "").
			add("qClass", qClass != null?qClass.name(): "").
			build();
	}	

}

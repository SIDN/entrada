package nl.sidn.dnslib.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;

import nl.sidn.dnslib.message.records.ResourceRecord;
import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;

public class RRset implements Serializable {

	private static final long serialVersionUID = -8053869237612837919L;
	
	private List<ResourceRecord> data = new ArrayList<>();
	private String owner;
	private ResourceRecordClass classz;
	private ResourceRecordType type;
	
	public RRset(){}
	
	public RRset(String owner, ResourceRecordClass classz,ResourceRecordType type) {
		this.owner = owner;
		this.classz = classz;
		this.type = type;
	}
	
	public static RRset createAs(ResourceRecord rr){
		RRset rrset = new RRset(rr.getName(), rr.getClassz(), rr.getType());
		rrset.add(rr);
		return rrset;
	}
	
	public String getOwner() {
		return owner;
	}

	public ResourceRecordClass getClassz() {
		return classz;
	}

	public ResourceRecordType getType() {
		return type;
	}
	
	public void add(ResourceRecord rr){
		if(rr.getName() == null){
			throw new IllegalArgumentException("Trying to add an Invalid rr to the rrset: " + rr);
		}
		if(rr.getClassz() == classz &&
				rr.getType() == type &&
				rr.getName().equalsIgnoreCase(owner)){
			data.add(rr);
		}else{
			throw new IllegalArgumentException("Trying to add an Invalid rr to the rrset: " + rr);
		}
	}
	
	public void remove(ResourceRecord rr){
		if(rr.getClassz() == classz &&
				rr.getType() == type &&
				rr.getName().equalsIgnoreCase(owner)){
			data.remove(rr);
		}else{
			throw new IllegalArgumentException("Trying to remove an Invalid rr from the rrset: " + rr);
		}
	}
	
	public void clear(){
		data.clear();
	}
	
	public List<ResourceRecord> getAll(){
		return data;
	}
	
	public int size(){
		return data.size();
	}

	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("RRset\n [ owner=" + owner + ", classz=" + classz + ", type=" + type + "\n");
		
		for (ResourceRecord rr : data) {
			b.append(rr.toString());
			b.append("\n");
		}

		b.append(" ]");
		
		return b.toString();
	}

	public Object toZone(int maxLength) {
		StringBuffer b = new StringBuffer();
		
		for (ResourceRecord rr : data) {
			b.append(rr.toZone(maxLength));
			b.append("\n");
		}

		
		return b.toString();
	}

	public JsonArray toJSon(){
		JsonArrayBuilder builder = Json.createArrayBuilder();
		
		for (ResourceRecord rr : data) {
			builder.add(rr.toJSon());
		}
		
		return builder.build();
	}

}

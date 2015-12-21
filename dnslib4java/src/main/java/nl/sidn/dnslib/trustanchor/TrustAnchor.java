package nl.sidn.dnslib.trustanchor;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="TrustAnchor")
public class TrustAnchor {

	private String id;
	private String source;
	
	private String zone;
	private KeyDigest keyDigest;
	
	@XmlAttribute
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	@XmlAttribute
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	
	@XmlElement(name="KeyDigest")
	public KeyDigest getKeyDigest() {
		return keyDigest;
	}
	public void setKeyDigest(KeyDigest keyDigest) {
		this.keyDigest = keyDigest;
	}
	public String getZone() {
		return zone;
	}
	@XmlElement(name="Zone")
	public void setZone(String zone) {
		this.zone = zone;
	}
	
	
	

}

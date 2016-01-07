/*
 * ENTRADA, a big data platform for network data analytics
 *
 * Copyright (C) 2016 SIDN [https://www.sidn.nl]
 * 
 * This file is part of ENTRADA.
 * 
 * ENTRADA is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ENTRADA is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ENTRADA.  If not, see [<http://www.gnu.org/licenses/].
 *
 */	
package nl.sidn.dnslib.trustanchor;

import java.util.Date;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

public class KeyDigest {

	private int keyTag;
	private int algorith;
	private int digestType;
	private String digest;
	
	private String id;
	private Date validFrom;
	private Date validUntil;
	
	@XmlElement(name="KeyTag")
	public int getKeyTag() {
		return keyTag;
	}
	public void setKeyTag(int keyTag) {
		this.keyTag = keyTag;
	}
	public int getAlgorith() {
		return algorith;
	}
	
	@XmlElement(name="Algorithm")
	public void setAlgorith(int algorith) {
		this.algorith = algorith;
	}
	
	@XmlElement(name="DigestType")
	public int getDigestType() {
		return digestType;
	}
	public void setDigestType(int digestType) {
		this.digestType = digestType;
	}
	@XmlElement(name="Digest")
	public String getDigest() {
		return digest;
	}
	public void setDigest(String digest) {
		this.digest = digest;
	}
	@XmlAttribute
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	@XmlAttribute
	public Date getValidFrom() {
		return validFrom;
	}
	public void setValidFrom(Date validFrom) {
		this.validFrom = validFrom;
	}
	@XmlAttribute
	public Date getValidUntil() {
		return validUntil;
	}
	public void setValidUntil(Date validUntil) {
		this.validUntil = validUntil;
	}

	
	

}

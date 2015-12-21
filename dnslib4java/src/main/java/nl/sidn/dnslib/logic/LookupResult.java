package nl.sidn.dnslib.logic;

import nl.sidn.dnslib.message.Message;
import nl.sidn.dnslib.types.RcodeType;
import nl.sidn.dnslib.types.ResourceRecordClass;
import nl.sidn.dnslib.types.ResourceRecordType;

public class LookupResult {
	
	private Message packet;
	/* text string, original question */
	private String qName;
	/* type code asked for */
	private ResourceRecordType qType;
	/* class code asked for */
	private ResourceRecordClass qClazz;
	/* canonical name of result */
	private String canconname;
	 /* additional error code in case of no data */
	private RcodeType rcode;
	 /* full network format answer packet */
	private byte[] datapacket;
	/* length of packet in octets */
	private int datapacketLength;
	 /* true if there is data */
	private boolean haveData;
	/* true if nodata because name does not exist */
	private boolean nxDomain;
	/* true if result is secure */
	private boolean secure;
	 /* true if a security failure happened */
	private boolean bogus;
	 /* string with error if bogus */
	private String whyBogus;
	/* status msg from resolver */
	private String status;
	/* if true then query was successful, otherwise false */
	private boolean ok;
	
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
	public ResourceRecordClass getqClazz() {
		return qClazz;
	}
	public void setqClazz(ResourceRecordClass qClazz) {
		this.qClazz = qClazz;
	}
	public String getCanconname() {
		return canconname;
	}
	public void setCanconname(String canconname) {
		this.canconname = canconname;
	}
	public RcodeType getRcode() {
		return rcode;
	}
	public void setRcode(RcodeType rcode) {
		this.rcode = rcode;
	}
	public byte[] getDatapacket() {
		return datapacket;
	}
	public void setDatapacket(byte[] datapacket) {
		this.datapacket = datapacket;
	}
	public int getDatapacketLength() {
		return datapacketLength;
	}
	public void setDatapacketLength(int datapacketLength) {
		this.datapacketLength = datapacketLength;
	}
	public boolean isHaveData() {
		return haveData;
	}
	public void setHaveData(boolean haveData) {
		this.haveData = haveData;
	}
	public boolean isNxDomain() {
		return nxDomain;
	}
	public void setNxDomain(boolean nxDomain) {
		this.nxDomain = nxDomain;
	}
	public boolean isSecure() {
		return secure;
	}
	public void setSecure(boolean secure) {
		this.secure = secure;
	}
	public boolean isBogus() {
		return bogus;
	}
	public void setBogus(boolean bogus) {
		this.bogus = bogus;
	}
	public String getWhyBogus() {
		return whyBogus;
	}
	public void setWhyBogus(String whyBogus) {
		this.whyBogus = whyBogus;
	}
	public Message getPacket() {
		return packet;
	}
	public void setPacket(Message packet) {
		this.packet = packet;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public boolean isOk() {
		return ok;
	}
	public void setOk(boolean ok) {
		this.ok = ok;
	}


}

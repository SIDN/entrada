package nl.sidn.pcap.packet;

public class ICMPPacket extends Packet{
	
	private short type;
	private short code;
	//contains IP hdr and (partial) dns response
	private Packet originalIPPacket;
	private boolean error;
	private boolean info;
	private int clientType;
	
	
	public short getType() {
		return type;
	}
	public void setType(short type) {
		this.type = type;
	}
	public short getCode() {
		return code;
	}
	public void setCode(short code) {
		this.code = code;
	}
	public Packet getOriginalIPPacket() {
		return originalIPPacket;
	}
	public void setOriginalIPPacket(Packet originalIPPacket) {
		this.originalIPPacket = originalIPPacket;
	}
	public boolean isError() {
		return error;
	}
	public void setError(boolean error) {
		this.error = error;
	}
	public boolean isInfo() {
		return info;
	}
	public void setInfo(boolean info) {
		this.info = info;
	}
	public int getClientType() {
		return clientType;
	}
	public void setClientType(int clientType) {
		this.clientType = clientType;
	}
	@Override
	public String toString() {
		return "ICMPPacket [type=" + type + ", code=" + code
				+ ", originalIPPacket=" + originalIPPacket + ", error=" + error
				+ ", info=" + info + ", clientType=" + clientType + "(" + super.toString() + ")]";
	}


}

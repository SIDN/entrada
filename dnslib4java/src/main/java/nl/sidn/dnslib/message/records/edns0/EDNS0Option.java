package nl.sidn.dnslib.message.records.edns0;

public class EDNS0Option {
	
	protected int  code;
	protected int len;
	
	public EDNS0Option(){}
	
	public EDNS0Option(int code, int len) {
		this.code = code;
		this.len = len;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
	}

	@Override
	public String toString() {
		return "EDNS0Option [code=" + code + ", len=" + len + "]";
	}
	
	

}

package nl.sidn.dnslib.message.records.edns0;


/**
 * http://tools.ietf.org/html/draft-vandergaast-edns-client-subnet-02
 *
 */
public class ClientSubnetOption extends EDNS0Option{

	private int fam;
	private int sourcenetmask;
	private int scopenetmask;
	private String address;
	
	public ClientSubnetOption(){}
	
	public ClientSubnetOption(int code, int len) {
		super(code, len);
	}

	public int getFam() {
		return fam;
	}

	public void setFam(int fam) {
		this.fam = fam;
	}

	public int getSourcenetmask() {
		return sourcenetmask;
	}

	public void setSourcenetmask(int sourcenetmask) {
		this.sourcenetmask = sourcenetmask;
	}

	public int getScopenetmask() {
		return scopenetmask;
	}

	public void setScopenetmask(int scopenetmask) {
		this.scopenetmask = scopenetmask;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	
	public boolean isIPv4(){
		return fam == 1;
	}
	
	public String export(){
		return (fam == 1? "4,": "6,") + address + "/" + sourcenetmask + "," + scopenetmask;
	}

	@Override
	public String toString() {
		return "ClientSubnetOption [fam=" + fam + ", sourcenetmask="
				+ sourcenetmask + ", scopenetmask=" + scopenetmask
				+ ", address=" + address + ", code=" + code + ", len=" + len
				+ "]";
	}

}

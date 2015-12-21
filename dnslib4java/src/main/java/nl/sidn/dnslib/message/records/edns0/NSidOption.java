package nl.sidn.dnslib.message.records.edns0;



/**
 * @see http://tools.ietf.org/html/rfc5001
 * 
 *
 */
public class NSidOption extends EDNS0Option{
	
	private String id;

	public NSidOption(){}
	
	public NSidOption(int code, int len) {
		super(code, len);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "NSidOption [id=" + id + ", code=" + code + ", len=" + len + "]";
	}


}

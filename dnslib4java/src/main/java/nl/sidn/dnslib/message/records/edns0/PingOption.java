package nl.sidn.dnslib.message.records.edns0;

import java.util.Arrays;


/**
 * @see http://tools.ietf.org/html/draft-hubert-ulevitch-edns-ping-01
 * 
 *
 */
public class PingOption extends EDNS0Option{
	
	private byte[] ping;
	
	public PingOption(){}

	public PingOption(int code, int len) {
		super(code, len);
	}

	public byte[] getPing() {
		return ping;
	}

	public void setPing(byte[] ping) {
		this.ping = ping;
	}

	@Override
	public String toString() {
		return "PingOption [ping=" + Arrays.toString(ping) + ", code=" + code
				+ ", len=" + len + "]";
	}




}

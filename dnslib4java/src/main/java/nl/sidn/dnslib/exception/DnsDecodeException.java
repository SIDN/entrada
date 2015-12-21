package nl.sidn.dnslib.exception;


public class DnsDecodeException extends RuntimeException{

	private static final long serialVersionUID = -2576098971422457470L;

	public DnsDecodeException(String msg){
		super(msg);
	}

}

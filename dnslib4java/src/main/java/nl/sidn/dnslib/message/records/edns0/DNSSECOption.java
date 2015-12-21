package nl.sidn.dnslib.message.records.edns0;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * @see http://tools.ietf.org/html/rfc6014
 *
 */
public class DNSSECOption extends EDNS0Option{
	
	public final static int OPTION_CODE_DAU = 5;
	public final static int OPTION_CODE_DHU = 6;
	public final static int OPTION_CODE_N3U = 7;
	
	private List<Integer> algs = new ArrayList<>();

	public DNSSECOption(int code, int len) {
		super(code, len);
	}
	
	public void addAlgorithm(int alg){
		algs.add(alg);
	}
	public List<Integer> getAlgorithms() {
		return algs;
	}

	@Override
	public String toString() {
		return "DNSSECOption [algs=" + algs + ", code=" + code + ", len=" + len
				+ "]";
	}
	
	public String export(){
		return StringUtils.join(algs, ',');
	}

}

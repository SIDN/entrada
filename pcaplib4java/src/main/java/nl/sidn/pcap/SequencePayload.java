package nl.sidn.pcap;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * Class for re-assembly of TCP fragments
 * @author maarten
 *
 */
public class SequencePayload implements Comparable<SequencePayload> {
	private Long seq;
	private byte[] payload;
	private long time;
	
	public SequencePayload(){}

	public SequencePayload(Long seq, byte[] payload,long time) {
		this.seq = seq;
		this.payload = payload;
		this.time = time;
	}

	@Override
	public int compareTo(SequencePayload o) {
		return ComparisonChain.start().compare(seq, o.seq)
		                              .compare(payload.length, o.payload.length)
		                              .result();
	}

	public boolean linked(SequencePayload o) {
		if ((seq + payload.length) == o.seq)
			return true;
		if ((o.seq + o.payload.length) == seq)
			return true;
		return false;
	}

	
	public long getTime() {
		return time;
	}

	public byte[] getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("seq", seq)
		                                              .add("len", payload.length)
		                                              .toString();
	}
}

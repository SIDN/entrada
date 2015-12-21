package nl.sidn.pcap.packet;


import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

public class DatagramPayload implements Comparable<DatagramPayload> {
	private Long offset;
	private byte[] payload;

	public DatagramPayload(Long offset, byte[] payload) {
		this.offset = offset;
		this.payload = payload;
	}

	@Override
	public int compareTo(DatagramPayload o) {
		return ComparisonChain.start().compare(offset, o.offset)
		                              .compare(payload.length, o.payload.length)
		                              .result();
	}

	public boolean linked(DatagramPayload o) {
		if ((offset + payload.length) == o.offset)
			return true;
		if ((o.offset + o.payload.length) == offset)
			return true;
		return false;
	}
	
	public Long getOffset() {
		return offset;
	}

	public byte[] getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this.getClass()).add("offset", offset)
		                                              .add("len", payload.length)
		                                              .toString();
	}
}
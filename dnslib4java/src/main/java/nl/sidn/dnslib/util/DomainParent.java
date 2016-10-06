package nl.sidn.dnslib.util;

public class DomainParent{
	private String match;
	private String parent;
	private int labels;
	
	public DomainParent(String match, String parent, int labels) {
		this.match = match;
		this.parent = parent;
		this.labels = labels;
	}

	public String getMatch() {
		return match;
	}

	public String getParent() {
		return parent;
	}

	public int getLabels() {
		return labels;
	}

	@Override
	public String toString() {
		return "DomainParent [match=" + match + ", parent=" + parent
				+ ", labels=" + labels + "]";
	}
	
	
}
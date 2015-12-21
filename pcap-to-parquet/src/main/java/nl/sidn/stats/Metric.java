package nl.sidn.stats;

public class Metric {
	
	private String name;
	private long value;
	private long time;
	
	public Metric(){}
	
	public Metric(String name, long value, long time) {
		this.name = name;
		this.value = value;
		this.time = time;
	}

	public String getName() {
		return name;
	}

	public long getValue() {
		return value;
	}
	
	public void update(long value){
		this.value += value;
	}

	public long getTime() {
		return time;
	}

	@Override
	public String toString() {
		return name + " " + value + " " + time;
	}

}

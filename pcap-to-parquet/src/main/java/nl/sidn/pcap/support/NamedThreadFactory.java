package nl.sidn.pcap.support;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {

	private String name;
	private long count = 1;
	private ThreadFactory factory = Executors.defaultThreadFactory();
	
	public NamedThreadFactory(String name){
		this.name = name;
	}
	
	@Override
	public Thread newThread(Runnable r) {
		Thread newThread = factory.newThread(r);
		newThread.setName(name + "-" + count);
		count++;
		return newThread;
	}
	
	
}

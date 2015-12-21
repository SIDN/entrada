package nl.sidn.pcap.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractStoppableThread implements Runnable{
	
	public static final Log LOG = LogFactory.getLog(AbstractStoppableThread.class);

	private boolean keepRunning = true;
	
	@Override
	public void run() {
		try{
			doWork();
		}catch(Exception e){
			LOG.error("Thread threw exception", e);
			throw new RuntimeException(e);
		}
	}

	protected abstract void doWork();

	public void stop() {
		keepRunning = false;
	}
	
	public boolean isRunning(){
		return keepRunning;
	}
	
}

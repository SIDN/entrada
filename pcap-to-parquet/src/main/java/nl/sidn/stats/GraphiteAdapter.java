package nl.sidn.stats;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.sidn.pcap.util.Settings;

/**
 * Adapter for sending metrics to the carbon database used by Graphite.
 *
 */
public class GraphiteAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteAdapter.class);
	
	private Socket socket = null;
	private PrintWriter writer = null;
	
	public void connect(){
		int serverPort = Settings.getInstance().getIntSetting("graphite.port");
		int timeout = Settings.getInstance().getIntSetting("graphite.connect.timeout");
		try {
			InetAddress host = InetAddress.getByName(Settings.getInstance().getSetting("graphite.host")); 
			socket = new Socket();
			socket.connect(new InetSocketAddress(host,serverPort),timeout*1000); 
			writer = new PrintWriter(socket.getOutputStream(),true);
		} catch (Exception e) {
			LOGGER.error("Error connecting to Graphite",e);
			//ignore the error, it's ok to miss some metrics in graphite
			//missing some metrics if preferred instead of crashing the application
		} 
	}
	
	public void send(String data){
		if(writer != null && socket.isConnected()){
			writer.print(data);
			writer.flush();
		}
	}
	
	public void close(){
		try {
			if(writer != null){
				writer.close();
			}
			if(socket != null){
				socket.close();
			}
		} catch (Exception e) {
			//just log
			LOGGER.error("Error closing connection to Graphite",e);
		}
	}

	public boolean isConnected(){
		return socket != null && !socket.isClosed() && socket.isConnected();
	}
}

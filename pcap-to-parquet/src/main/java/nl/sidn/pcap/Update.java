package nl.sidn.pcap;
import nl.sidn.pcap.ip.GoogleResolverCheck;
import nl.sidn.pcap.ip.OpenDNSResolverCheck;
import nl.sidn.pcap.util.Settings;


/**
 * Update the config data used by the pcap2parquet processing.
 *
 */
public class Update {

	public static void main(String[] args) {
		new Update().update(args);
	}
	
	public void update(String[] args){
		
		if(args == null || args.length < 2){
			throw new RuntimeException("Incorrect number of parameters found.");
		}
		
		Settings.setPath(args[0]);
		//set state location
		Settings.getInstance().setSetting(Settings.STATE_LOCATION, args[1]);
		//Google and OpenDNS checks will update when check object is created
		new GoogleResolverCheck().update();
		new OpenDNSResolverCheck().update();
	}
}

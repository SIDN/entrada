package nl.sidn.pcap.util;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FileUtil.class);
	
	public static int countFiles(String inputDir) {
		LOGGER.info("Scan for pcap files in: " + inputDir);
		File f = FileUtils.getFile(inputDir);
		Iterator<File> files = FileUtils.iterateFiles(f, new String[]{"pcap.gz"}, false);
		int filecount = 0;
	    while(files.hasNext()) {
	    	files.next();
	    	filecount++;
	    }
	    return filecount;
	}

}

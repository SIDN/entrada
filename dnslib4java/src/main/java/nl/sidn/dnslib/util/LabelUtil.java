package nl.sidn.dnslib.util;

import org.apache.commons.lang3.StringUtils;

public class LabelUtil {
	
	public static int count(String name){
		if(name == null || ".".equals(name)){
			return 0;
		}
		
		return StringUtils.split(name, ".").length;
		
	}
	
	public static String stripFirstLabel(String name){
		
		String[] parts = StringUtils.split(name, ".");
		if(parts != null && parts.length == 1){
			return ".";
		}
		else if(parts != null && parts.length > 1){
			StringBuffer b = new StringBuffer();
			boolean adddot = false;
			for (int i = 1; i < parts.length; i++) {
				if(adddot){
					b.append(".");
				}
				b.append(parts[i]);
				
				adddot = true;
				
			}
			if(name.endsWith(".")){
				b.append(".");
			}
			return b.toString();
		}
		
		return null;
		
	}

}

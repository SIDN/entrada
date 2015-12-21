package nl.sidn.dnslib.util;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class NameUtil {
	
	private static Pattern FQDN;
	private static final String DOMAIN_NAME_PATTERN = "^((?!-)[A-Za-z0-9-_]{1,63}(?<!-)\\.)*([A-Za-z]{2,2})*(\\.)*$";
	
	static {
		FQDN = Pattern.compile(DOMAIN_NAME_PATTERN);
	}
 

	public static String reverse(String name) {
		String[] parts = StringUtils.split(name, '.');
		if (parts.length > 0) {
			String reverse = "";
			for (int i = parts.length - 1; i >= 0; i--) {
				reverse = reverse + "." + parts[i];
			}
			return reverse;
		}
		return null;
	}
	
	public static Domaininfo getDomain(String name){
		if(name == null || name.length() == 0){
			return new Domaininfo(null,0);
		}
		if(StringUtils.equals(name, ".")){
			return new Domaininfo(name,0);
		}
		
		String[] parts = StringUtils.split(name, ".");

		if(parts != null && parts.length > 0){
			if( parts.length == 1){
				return new Domaininfo(parts[parts.length-1],parts.length);
			}
			//all other
			return new Domaininfo(parts[parts.length-2] + "." + parts[parts.length-1],parts.length);
		}
		
		return new Domaininfo(name,0);
		
	}
	
	public static boolean isFqdn(String domain){
		return FQDN.matcher(domain).find();
	}
	
	public static String getSecondLevel(String name) {
		if(name == null || name.length() == 0){
			return null;
		}
		
		String[] parts = StringUtils.split(name, '.');
		if (parts.length == 0) {
			return name;
		}else if (parts.length == 1) {
			 return parts[parts.length-1];
		}else {
			return parts[parts.length-2]  + "." + parts[parts.length-1];
		}
		
	}
	


}

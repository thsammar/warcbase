package org.warcbase.cwi.pig.piggybank;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.google.common.net.InternetDomainName;

public class GetHostName extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.get(0) == null) {
			return null;
		}
		String domain = null;
		try {
			
			String url = ((String) input.get(0));
			URI uri = new URI(url.trim());
			//String host = uri.getHost();
			//System.out.println(host);
			if (uri.getHost()!=null){
			InternetDomainName idn = InternetDomainName.from(uri.getHost());
			domain = (uri.getHost());
			
			return domain.startsWith("www.") ? domain.substring(4) : domain ;
			//hostName =idn.parent().name(); 
					//idn.topPrivateDomain().name();
			//return domain;
			}
			else
				return domain;

		} catch (IllegalArgumentException e) {
			return domain;
		} catch (IllegalStateException e) {
			return domain;
		} catch (URISyntaxException e) {
			return domain;
		}

	}
}




//try {
//	String hostName = "";
//	URL siteURL = new URL((String) input.get(0));
//	String hostNameURL = siteURL.getHost().toLowerCase();
//	String[] hostNameArr = hostNameURL.split("\\.");
//	if (hostNameArr.length >= 2) {
//		hostName = hostNameArr[hostNameArr.length - 2];
//		return hostName;
//	} else
//		hostName = (String) input.get(0);
//	return hostName;
//
//} catch (java.net.MalformedURLException mue) {
//	return null;
//}

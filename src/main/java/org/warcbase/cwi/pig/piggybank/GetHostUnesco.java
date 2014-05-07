package org.warcbase.cwi.pig.piggybank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class GetHostUnesco extends EvalFunc<String> {
	
	String metaFile;
    Map<String, String> UnescoCodeMapping = null;
	public GetHostUnesco(String file) {
        // Just store the filename. Don't load the lookup table, since we may
        // be on the frontend or the backend.
        metaFile = file;
    }
	
	
	
	public String exec(Tuple input) throws IOException {
	    if (input == null || input.size() != 2 || input.get(0) == null || input.get(1) == null) {
	      return null;
	    }

	   
		try {
			UnescoCodeMapping = new HashMap<String, String>(30);
			BufferedReader br = new BufferedReader(new FileReader(
					"./meta_unescolookup"));
			String line = null;

			while ((line = br.readLine()) != null) {

				
				String[] lineComp = line.split("\t");
				if (lineComp.length > 1) {
					UnescoCodeMapping.put(getHostname(lineComp[3].trim()),
							lineComp[0].trim());
				}
			}
			br.close();
			
			return UnescoCodeMapping.get((String)input.get(0));
			
		} catch (Exception e) {
			return null;
		}
		
	  }
	
	public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        // We were passed the name of the file on HDFS.  Append a
        // name for the file on the task node.
        list.add(metaFile + "#meta_unescolookup");
        return list;
    }

	private String getHostname(String url)
			throws java.net.MalformedURLException {
		// decode url, get hostname
		String hostName = "";
		try {
			URL siteURL = new URL(url);
			String hostNameURL = siteURL.getHost().toLowerCase();
			String[] hostNameArr = hostNameURL.split("\\.");
			if (hostNameArr.length >= 2) {
				hostName = hostNameArr[hostNameArr.length - 2];
			} else {
				hostName = url;
			}
		} catch (java.net.MalformedURLException mue) {

			hostName = url;
		}
		return hostName;
	}
}
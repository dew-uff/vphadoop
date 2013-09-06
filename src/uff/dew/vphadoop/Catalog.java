package uff.dew.vphadoop;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;


public class Catalog {

	private static final String cardinalityQuery = "let $elm := doc('#')? return count($elm)";
	
	private static Catalog _instance = null;
	
	// db connection info
	private String databaseHost = null;
	private int databasePort = -1;
	private String databaseUsername = null;
	private String databasePassword = null;
	
	private BaseXClient client = null;
	
	private Catalog(Configuration conf) {
	    
	    databaseHost = conf.get(XmlDBConst.DB_HOST);
	    databasePort = Integer.parseInt(conf.get(XmlDBConst.DB_PORT));
	    databaseUsername = conf.get(XmlDBConst.DB_USER);
	    databasePassword = conf.get(XmlDBConst.DB_PASSWORD);
	    
		try {
			client = new BaseXClient(databaseHost, databasePort, databaseUsername, databasePassword);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Catalog get(Configuration conf) {
		if (_instance == null) {
			_instance = new Catalog(conf);
		}
		return _instance;
	}
	
	public int getCardinality(String doc, String element) {
		String result = "";
		try {
			String query = cardinalityQuery.replace("#", doc);
			query = query.replace("?", element);
			result = client.executeXQuery(query);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Integer.parseInt(result);
	}
	
}

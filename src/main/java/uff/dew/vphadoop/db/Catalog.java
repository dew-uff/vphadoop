package uff.dew.vphadoop.db;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.xquery.XQException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import uff.dew.vphadoop.VPConst;


public class Catalog {
    
    public static final Log LOG = LogFactory.getLog(Catalog.class);

	private static final String cardinalityQuery = "let $elm := # return count($elm)";
	
	private static Catalog _instance = null;
	
	Database database;
	
	private Catalog() {
	}

	public static Catalog get() {
		if (_instance == null) {
			_instance = new Catalog();
		}
		return _instance;
	}

    public void setConfiguration(Configuration conf) throws IOException {
        
        String configFile = conf.get(VPConst.DB_CONFIGFILE_PATH);
        LOG.info("setConfiguration() " + configFile);
        FileSystem dfs = FileSystem.get(conf);
        InputStream is = dfs.open(new Path(configFile));
        
        database = DatabaseFactory.createDatabase(is);
    }
	
	public int getCardinality(String xpath) throws IOException {
	    
	    LOG.info("xpath: " + xpath);
		try {
            String query = cardinalityQuery.replace("#", xpath);
            String result = database.executeQueryAsString(query);
            
            return Integer.parseInt(result);
        } catch (XQException e) {
            throw new IOException(e);
        }
	}
	
	public Database getDatabase() {
	    return database;
	}
}

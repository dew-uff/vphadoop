package uff.dew.vphadoop.db;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

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
	
	XQDataSource dataSource;
	
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
        
        dataSource = DataSourceFactory.createDataSource(is);
    }
	
	public int getCardinality(String xpath) throws IOException {
		try {
            String query = cardinalityQuery.replace("#", xpath);
            XQConnection conn = dataSource.getConnection();
            XQPreparedExpression xpe = conn.prepareExpression(query);
            XQResultSequence rseq = xpe.executeQuery();
            rseq.next();
            return rseq.getInt();
        } catch (XQException e) {
            throw new IOException(e);
        }
	}
}

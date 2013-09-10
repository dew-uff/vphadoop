package uff.dew.vphadoop.db;
import java.io.IOException;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import org.apache.hadoop.conf.Configuration;

import uff.dew.vphadoop.VPConst;


public class Catalog {

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
        
        dataSource = DataSourceFactory.createDataSource(configFile);
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

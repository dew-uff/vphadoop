package uff.dew.vphadoop.db;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class BaseDatabase implements Database {
	
	private static Log LOG = LogFactory.getLog(BaseDatabase.class);
    
    protected XQDataSource dataSource;
    
    public XQResultSequence executeQuery(String query) throws XQException {
    	LOG.debug("Query: " + query);
    	long start = System.currentTimeMillis();
        XQConnection conn = dataSource.getConnection();
        XQPreparedExpression exp = conn.prepareExpression(query);
        XQResultSequence result = exp.executeQuery();
        LOG.debug("Query execution time: " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }
    
    public String executeQueryAsString(String query) throws XQException {
        return stringalizeResult(executeQuery(query));
    }
    
    protected XQConnection getConnection() throws XQException {
        return dataSource.getConnection();
    }
    
    public static String stringalizeResult(XQResultSequence rs) throws XQException {
        
        if (rs != null) {
            return rs.getSequenceAsString(null);           
        }
        
        return null;
    }
}

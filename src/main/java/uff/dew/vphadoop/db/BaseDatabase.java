package uff.dew.vphadoop.db;

import java.util.HashMap;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class BaseDatabase implements Database {
	
	private class QueryExecution {
		private String query;
		private XQConnection conn;
		private XQPreparedExpression prepExp;
		private XQResultSequence rs;
		
		public QueryExecution(String query) {
			this.query = query;
		}
		
		public XQResultSequence executeQuery() throws XQException {
			conn = dataSource.getConnection();
			prepExp = conn.prepareExpression(query);
			rs = prepExp.executeQuery();
			
			return rs;
		}
		
		public void close() throws XQException {
			if (rs != null) {
				rs.close();
			}
			if (prepExp != null) {
				prepExp.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}
	
	private HashMap<XQResultSequence, QueryExecution> queriesInExecution = new HashMap<XQResultSequence,QueryExecution>();
	
	private static Log LOG = LogFactory.getLog(BaseDatabase.class);
    
    protected XQDataSource dataSource;
    protected String databaseName;
    
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String name) {
        this.databaseName = name;
    }

    public XQResultSequence executeQuery(String query) throws XQException {
    	LOG.debug("Query: " + query);
    	long start = System.currentTimeMillis();
    	QueryExecution qe = new QueryExecution(query);
    	XQResultSequence result = qe.executeQuery();
    	queriesInExecution.put(result, qe);
    	LOG.debug("Query id: " + result);
    	LOG.debug("Query execution time: " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }

	public String executeQueryAsString(String query) throws XQException {
        XQResultSequence rs = executeQuery(query);
        String result = stringalizeResult(rs);
        freeResources(rs);
		return result;
    }
    
    protected XQConnection getConnection() throws XQException {
        return dataSource.getConnection();
    }
    
    public void freeResources(XQResultSequence rs) throws XQException {
    	QueryExecution qe = queriesInExecution.remove(rs);
    	if (qe != null) {
    		LOG.debug("Freeing resouces for query id: " + rs);
    		qe.close();
    	}
    }
    
    public static String stringalizeResult(XQResultSequence rs) throws XQException {
        
        String result = null;
    	if (rs != null) {
            result = rs.getSequenceAsString(null);           
        }
        return result;
    }
}

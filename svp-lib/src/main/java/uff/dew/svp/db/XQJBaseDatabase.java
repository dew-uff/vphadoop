package uff.dew.svp.db;

import java.util.HashMap;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQExpression;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class XQJBaseDatabase implements Database {
	
	private class ExecutionContext {
		private XQPreparedExpression prepExp = null;
		private XQExpression exp = null;
		private XQResultSequence rs = null;
		private boolean closeSession = true;
		
		public XQResultSequence executeQuery(String query) throws XQException {
			closeSession = openSession();
			prepExp = conn.prepareExpression(query);
			rs = prepExp.executeQuery();
			
			return rs;
		}

		public void executeCommand(String command) throws XQException {
			closeSession = openSession();
			exp = conn.createExpression();
			exp.executeCommand(command);
		}

		
		public void close() throws XQException {
			if (rs != null) {
				rs.close();
			}
			if (prepExp != null) {
				prepExp.close();
			}
			if (exp != null) {
				exp.close();
			}
			if (closeSession) {
				closeSession();
			}
		}
	}
	
	private static Log LOG = LogFactory.getLog(XQJBaseDatabase.class);
    
    protected XQDataSource dataSource;
    protected String databaseName;
	private HashMap<XQResultSequence, ExecutionContext> queriesInExecution = new HashMap<XQResultSequence,ExecutionContext>();
	private XQConnection conn;

	protected boolean openSession() throws XQException {
		if (conn == null) {
			LOG.debug("openSession");
			conn = dataSource.getConnection();
			return true;
		}
		return false;
	}
	
	protected void closeSession() throws XQException {
		LOG.debug("closeSession");
		conn.close();
		conn = null;
	}
	
    protected XQResultSequence baseExecuteQuery(String query) throws XQException {
    	LOG.debug("executeQuery: " + query);
    	long start = System.currentTimeMillis();
    	
    	ExecutionContext qe = new ExecutionContext();
    	XQResultSequence result = qe.executeQuery(query);
    	queriesInExecution.put(result, qe);
    	LOG.debug("QueryContext: " + result.hashCode());
    	LOG.debug("Query execution time: " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }

	protected String baseExecuteQueryAsString(String query) throws XQException {
        XQResultSequence rs = baseExecuteQuery(query);
        String result = stringalizeResult(rs);
        freeResources(rs);
		return result;
    }
	
    protected void baseExecuteCommand(String command) throws XQException {
        LOG.debug("command: " + command);
        long start = System.currentTimeMillis();
        ExecutionContext ec = new ExecutionContext();
        ec.executeCommand(command);
        ec.close();
        LOG.debug("Command execution time: " + (System.currentTimeMillis() - start) + " ms.");
    }
    
    protected void baseFreeResources(XQResultSequence rs) throws XQException {
    	ExecutionContext qe = queriesInExecution.remove(rs);
    	if (qe != null) {
    		LOG.debug("Freeing resouces for QueryContext: " + rs.hashCode());
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
    
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String name) {
        this.databaseName = name;
    }
}

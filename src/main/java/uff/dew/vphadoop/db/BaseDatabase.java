package uff.dew.vphadoop.db;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

public abstract class BaseDatabase implements Database {
    
    protected XQDataSource dataSource;
    
    public XQResultSequence executeQuery(String query) throws XQException {
        XQConnection conn = dataSource.getConnection();
        XQPreparedExpression exp = conn.prepareExpression(query);
        return exp.executeQuery();
    }
    
    public String executeQueryAsString(String query) throws XQException {
        return stringalizeResult(executeQuery(query));
    }
    
    protected XQConnection getConnection() throws XQException {
        return dataSource.getConnection();
    }
    
    public static String stringalizeResult(XQResultSequence rs) throws XQException {
        
        StringBuilder result = new StringBuilder();
        
        if (rs != null) {
            while (rs.next()) {               
                result.append(rs.getItemAsString(null));
            }            
        }
        
        return result.toString();
    }
}

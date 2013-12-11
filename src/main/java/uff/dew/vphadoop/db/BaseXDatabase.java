package uff.dew.vphadoop.db;

import java.io.IOException;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQExpression;

import net.xqj.basex.BaseXXQDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BaseXDatabase extends BaseDatabase {
	
	private static Log LOG = LogFactory.getLog(BaseXDatabase.class);

    public BaseXDatabase(String host, int port, String username, String password) throws IOException {
   	
    	BaseXXQDataSource basexDataSource = new BaseXXQDataSource();

        basexDataSource.setServerName(host);
        basexDataSource.setPort(port);
        basexDataSource.setUser(username);
        basexDataSource.setPassword(password);
        
        dataSource = basexDataSource;
    }
    
    @Override
    public void deleteCollection(String collectionName) throws XQException {
        executeCommand("DROP DB " + collectionName);
    }

    @Override
    public void createCollection(String collectionName) throws XQException {
        executeCommand("CREATE DB " + collectionName);
    }
    
    @Override
    public void createCollectionWithContent(String collectionName, String dirPath) 
            throws XQException {
        executeCommand("CREATE DB " + collectionName + " " + dirPath);
        
    }
    
    private void executeCommand(String command) throws XQException {
    	LOG.debug("Command: " + command);
    	long start = System.currentTimeMillis();
        XQConnection conn = getConnection();
        XQExpression exp = conn.createExpression();
        exp.executeCommand(command);
        LOG.debug("Command execution time: " + (System.currentTimeMillis() - start) + " ms.");
        exp.close();
        conn.close();
    }

    @Override
    public int getCardinality(String xpath, String document, String collection) {
        
        String query = "let $elm := doc('" + document + "')/" + xpath + " return count($elm)";
        
        int result = -1;
        try {
            result = Integer.parseInt(executeQueryAsString(query));            
        }
        catch (XQException e) {
            e.printStackTrace();
        }
        
        return result;
    }
    
    @Override
    public String getHost() {
        BaseXXQDataSource ds = (BaseXXQDataSource) dataSource;
        return ds.getServerName();
    }

    @Override
    public int getPort() {
        BaseXXQDataSource ds = (BaseXXQDataSource) dataSource;
        return Integer.parseInt(ds.getProperty("port"));
    }

    @Override
    public String getUsername() {
        BaseXXQDataSource ds = (BaseXXQDataSource) dataSource;
        return ds.getUser();
    }

    @Override
    public String getPassword() {
        BaseXXQDataSource ds = (BaseXXQDataSource) dataSource;
        return ds.getPassword();
    }

    @Override
    public void loadFileInCollection(String collectionName, String filePath)
            throws XQException {
        // TODO implement it
        throw new XQException("Not implemented! Have a nice day.");
        
    }

    @Override
    public String getType() {
        return DatabaseFactory.TYPE_BASEX;
    }

}

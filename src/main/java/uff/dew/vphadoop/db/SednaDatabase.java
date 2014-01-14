package uff.dew.vphadoop.db;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQExpression;

import net.xqj.basex.BaseXXQDataSource;
import net.xqj.sedna.SednaXQDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.vphadoop.catalog.Element;

public class SednaDatabase extends BaseDatabase {
    
    private static final Log LOG = LogFactory.getLog(SednaDatabase.class);

    public SednaDatabase(String host, int port, String username, String password, String database) throws IOException {
        SednaXQDataSource sednaDataSource = new SednaXQDataSource();

        sednaDataSource.setServerName(host);
        sednaDataSource.setPort(port);
        sednaDataSource.setUser(username);
        sednaDataSource.setPassword(password);
        sednaDataSource.setDatabaseName(database);
        
        dataSource = sednaDataSource;
    }
    
    @Override
    public void deleteCollection(String collectionName) throws XQException {
        if (existsCollection(collectionName)) {
            executeCommand("DROP COLLECTION '" + collectionName + "'");
        }
    }

    @Override
    public void createCollection(String collectionName) throws XQException {
        executeCommand("CREATE COLLECTION '" + collectionName + "'");
    }
    
    @Override
    public void createCollectionWithContent(String collectionName, String dirPath) 
            throws XQException {
        createCollection(collectionName);
        File dir = new File(dirPath);
        File[] files = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (file.getName().endsWith(".xml")) {
                    return true;
                }
                return false;
            }
        });
        for(File file: files) {
            loadFileInCollection(collectionName, file.getAbsolutePath());
        }
    }
    
    private void executeCommand(String command) throws XQException {
        LOG.debug("command: " + command);
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
        String query = "let $elm := doc('" + document + ((collection != null && collection.length() > 0)?("', '"+ collection):"") +"')/" + xpath + " return count($elm)";
        
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
        File file = new File(filePath);
        executeCommand("LOAD '" + filePath + "' '" + file.getName() + "' '" + collectionName + "'"); 
    }
    
    private boolean existsCollection(String collectionName) throws XQException {
        return getCardinality("/collections/collection[@name='"+collectionName+"']", "$collections", null) > 0?true:false;
    }
    
    @Override
    public String getType() {
        return DatabaseFactory.TYPE_SEDNA;
    }

	@Override
	public Map<String, List<Element>> getCatalog() {
		// TODO Auto-generated method stub
		return null;
	}
}

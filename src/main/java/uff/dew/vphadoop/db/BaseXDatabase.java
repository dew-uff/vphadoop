package uff.dew.vphadoop.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQExpression;
import javax.xml.xquery.XQResultSequence;

import net.xqj.basex.BaseXXQDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.vphadoop.catalog.Element;

public class BaseXDatabase extends BaseDatabase {
	
	private static Log LOG = LogFactory.getLog(BaseXDatabase.class);

    public BaseXDatabase(String host, int port, String username, String password, String database) throws IOException {
   	
    	BaseXXQDataSource basexDataSource = new BaseXXQDataSource();

        basexDataSource.setServerName(host);
        basexDataSource.setPort(port);
        basexDataSource.setUser(username);
        basexDataSource.setPassword(password);
        setDatabaseName(database);
        
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

    @Override
    public Map<String, Element> getCatalog() {

        String query = "index:facets('"+databaseName+"')";
        
        Map<String,Element> map = null;
        
        try {
            XQResultSequence seq = executeQuery(query);
            if (seq.next()) {
                XMLStreamReader stream = seq.getItemAsStream();
                map = parseIndexFacets(stream);                
            }
            freeResources(seq);
        } catch (XQException e) {
        	LOG.error("Error creating catalog! + " + e.getMessage());
        } catch (XMLStreamException e) {
        	LOG.error("Error creating catalog! + " + e.getMessage());
        } 

        return map;
    }

    private Map<String, Element> parseIndexFacets(XMLStreamReader stream) throws XMLStreamException {
        
        Element element = null;
        Map<String, Element> map = new HashMap<String, Element>();
        String currentPath = "";
        
        while (stream.hasNext()) {
            int type = stream.next();
            
            switch (type) {
            case XMLStreamReader.START_ELEMENT:
                if (stream.getLocalName() == "element") {
                    Element newElement = new Element();
                    if (element != null) {
                        newElement.setParent(element);
                    }
                    element = newElement;

                    String name = stream.getAttributeValue(null, "name");
                    String count = stream.getAttributeValue(null, "count");
                    if (count != null && name != null) {
                        int cardinality = Integer.parseInt(count);
                        currentPath += "/" + name;
                        element.setCount(cardinality);
                        element.setName(name);
                        element.setPath(currentPath);
                        map.put(element.getPath(), element);
                    }                    
                }

                break;
            case XMLStreamReader.END_ELEMENT:
                if (stream.getLocalName() == "element") {
                    currentPath = currentPath.substring(0, currentPath.lastIndexOf('/'));
                    if (element != null) {
                    	element = element.getParent();
                    }
                }
            }
        }            
        
        return map;
    }
}

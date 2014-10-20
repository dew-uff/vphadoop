package uff.dew.vphadoop.db;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.xqj.basex.BaseXXQDataSource;
import net.xqj.sedna.SednaXQDataSource;
import uff.dew.vphadoop.catalog.Element;

public class SednaDatabase extends BaseDatabase {
    
    private static Log LOG = LogFactory.getLog(SednaDatabase.class);

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
        boolean shouldCloseSession = openSession();
    	if (existsCollection(collectionName)) {
            executeCommand("DROP COLLECTION '" + collectionName + "'");
        }
    	if (shouldCloseSession) {
    		closeSession();
    	}
    }

    @Override
    public void createCollection(String collectionName) throws XQException {
        executeCommand("CREATE COLLECTION '" + collectionName + "'");
    }
    
    @Override
    public void createCollectionWithContent(String collectionName, String dirPath) 
            throws XQException {
    	boolean shouldCloseSession = openSession();
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
        if (shouldCloseSession) {
        	closeSession();
        }
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
	public Map<String, Element> getCatalog() {
        
	    LOG.debug("Getting catalog data from Sedna database: " + databaseName);
	   
	    long startTimestamp = System.currentTimeMillis();
	    
	    String query = "doc('$schema')";
        
        Map<String,Element> map = null;
        
        try {
            XQResultSequence seq = executeQuery(query);
            if (seq.next()) {
                XMLStreamReader stream = seq.getItemAsStream();
                map = parseSchemaDocument(stream);                
            }
            freeResources(seq);
        } catch (XQException e) {
            LOG.error("Error creating catalog! + " + e.getMessage());
        } catch (XMLStreamException e) {
            LOG.error("Error creating catalog! + " + e.getMessage());
        } 
        
        LOG.debug("Catalog created! Time to parse schema document: " + 
                (System.currentTimeMillis() - startTimestamp) + "ms.");
        
        return map;
	}
	
    private Map<String, Element> parseSchemaDocument(XMLStreamReader stream) throws XMLStreamException {
        
        Element element = null;
        Map<String, Element> map = new HashMap<String, Element>();
        String currentPath = "";
        
        while (stream.hasNext()) {
            int type = stream.next();
            
            switch (type) {
            case XMLStreamReader.START_ELEMENT:
                if (stream.getLocalName() == "element") {
                    // we got a new element
                    Element newElement = new Element();
                    if (element != null) {
                        // if current element is not null, that means the new element
                        // is subelement of the current element, so set parent
                        newElement.setParent(element);
                    }
                    // now new element is the current element
                    element = newElement;

                    String name = stream.getAttributeValue(null, "name");
                    String count = stream.getAttributeValue(null, "total_nodes");
                    if (count != null && name != null) {
                        int cardinality = Integer.parseInt(count);
                        // set current path to include this element
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
                    // we finished processing this element, return current path and
                    // current element to the values of the parent element
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

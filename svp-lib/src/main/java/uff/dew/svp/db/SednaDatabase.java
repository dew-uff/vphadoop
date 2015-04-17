package uff.dew.svp.db;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

import net.xqj.basex.BaseXXQDataSource;
import net.xqj.sedna.SednaXQDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.svp.catalog.Element;

public class SednaDatabase extends XQJBaseDatabase {
    
    private static Log LOG = LogFactory.getLog(SednaDatabase.class);

    public SednaDatabase(String host, int port, String username, String password, String database) {
        
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
            baseExecuteCommand("DROP COLLECTION '" + collectionName + "'");
        }
    	if (shouldCloseSession) {
    		closeSession();
    	}
    }

    @Override
    public void createCollection(String collectionName) throws XQException {
        baseExecuteCommand("CREATE COLLECTION '" + collectionName + "'");
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

        String resource = document + ((collection != null && collection.length() > 0)?("', '"+ collection):"");
        String resourceType = "doc";

        if (document == null) {
            // means we will be considering entire collection;
            resource = collection;
            resourceType = "collection";
        }
        String query = "let $elm := "+resourceType+"('" + resource +"')/" + xpath + " return count($elm)";
        
        int result = -1;
        try {
            result = Integer.parseInt(executeQueryAsString(query));            
        }
        catch (XQException e) {
            LOG.error("Execution error in getCardinality! " + e.getMessage());
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
        baseExecuteCommand("LOAD '" + filePath + "' '" + file.getName() + "' '" + collectionName + "'"); 
    }
    
    @Override
    public String getType() {
        return Constants.DB_TYPE_SEDNA;
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
	
    @Override
    public String[] getParentElement(String elementName, String collectionName,
            String docName) {
        
        String resource = (collectionName != null && collectionName.length() > 0)?collectionName:docName;
        
        if (resource == null) {
            return null;
        }
        
        String query = "for $n in doc('$schema_" + resource + "')//element"
                + " where $n/element/@name = \"" + elementName +"\""           
                + " return substring($n/@name,1)";

        String[] result = null;
        
        try {
            XQResultSequence rs = executeQuery(query);
            if (rs != null) {
                List<String> allParents = new ArrayList<String>();
                while(rs.next()) {
                    allParents.add(rs.getItemAsString(null).trim());
                }
                freeResources(rs);
                if (allParents.size() > 0) {
                    result = allParents.toArray(new String[0]);
                }
            }
        } catch (XQException e) {
            LOG.error("Something wrong while get parent element!!");                    
        }
        
        return result;
    }

    @Override
    public String[] getDocumentsNamesForCollection(String collectionName) {
        String query = "for $c in doc('$documents')//collection[@name='"+collectionName+"']/document/@name return concat( substring($c,1),',')";
        
        String[] documents = null;
        try {
            String result = executeQueryAsString(query);
            documents = result.split(",");
            for(int i = 0; i < documents.length; i++) {
                documents[i] = documents[i].trim();
            }
        } catch (XQException e) {
            LOG.error("Something wrong while getting documents names for collection!!");                    
        }
        return documents;
    }

    @Override
    public XQResultSequence executeQuery(String query) throws XQException {
        return baseExecuteQuery(query);
    }

    @Override
    public String executeQueryAsString(String query) throws XQException {
        return baseExecuteQueryAsString(query);
    }

    @Override
    public void freeResources(XQResultSequence rs) throws XQException {
        baseFreeResources(rs);
    }

    private boolean existsCollection(String collectionName) throws XQException {
        return getCardinality("/collections/collection[@name='"+collectionName+"']", "$collections", null) > 0?true:false;
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

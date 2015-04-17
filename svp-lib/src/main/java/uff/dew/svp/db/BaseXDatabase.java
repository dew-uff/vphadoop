package uff.dew.svp.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

import net.xqj.basex.BaseXXQDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.svp.catalog.Element;

public class BaseXDatabase extends XQJBaseDatabase {
	
	private static Log LOG = LogFactory.getLog(BaseXDatabase.class);

    public BaseXDatabase(String host, int port, String username, String password, String database) {
   	
    	BaseXXQDataSource basexDataSource = new BaseXXQDataSource();

        basexDataSource.setServerName(host);
        basexDataSource.setPort(port);
        basexDataSource.setUser(username);
        basexDataSource.setPassword(password);
        setDatabaseName(database);
        
        dataSource = basexDataSource;
    }
    
    @Override
    public XQResultSequence executeQuery(String query) throws XQException {
        
        query = preprocess(query);
        
        return baseExecuteQuery(query);
    }
    
    @Override
    public String executeQueryAsString(String query) throws XQException {

        query = preprocess(query);
        
        return baseExecuteQueryAsString(query);
    }
    
    @Override
    public void deleteCollection(String collectionName) throws XQException {
        baseExecuteCommand("DROP DB " + collectionName);
    }

    @Override
    public void createCollection(String collectionName) throws XQException {
        baseExecuteCommand("CREATE DB " + collectionName);
    }
    
    @Override
    public void createCollectionWithContent(String collectionName, String dirPath) 
            throws XQException {
        baseExecuteCommand("CREATE DB " + collectionName + " " + dirPath);
    }
    
    @Override
    public int getCardinality(String xpath, String document, String collection) {
        
        String docIdentifier = getDatabaseName() + "/" + document;
        if (collection != null && collection.length() > 0) {
            // BaseX considers a collection as a new database, so
            // we need to ignore database name and use collection as database name
            docIdentifier = collection + "/" + document;
        }
        String query = "let $elm := doc('" + docIdentifier + "')/" + xpath + " return count($elm)";
        
        int result = -1;
        try {
            result = Integer.parseInt(baseExecuteQueryAsString(query));            
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
        
        if (collectionName == null || filePath == null) {
            // TODO throw a DatabaseException
            return;
        }
        
        String[] parts = filePath.split("/");
        String docName = parts[parts.length-1];
        
        baseExecuteCommand("OPEN " + getDatabaseName() + " ; ADD TO " + docName + " " + filePath);
    }

    @Override
    public String getType() {
        return Constants.DB_TYPE_BASEX;
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

    @Override
    public String[] getParentElement(String elementName, String collectionName,
            String docName) {
        
        if (docName == null) {
            return null;
        }
        
        String resource = (collectionName != null && collectionName.length() > 0)?collectionName + "/"+docName : getDatabaseName() + "/" + docName;
        
        String query = "distinct-values(doc('" + resource + "')//" + elementName + "/../name()[1])";

        String[] result = null;
        
        try {
            XQResultSequence rs = baseExecuteQuery(query);
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
        
        if (collectionName == null) {
            return null;
        }
        
        String query = "for $d in collection('"+ collectionName + "') return document-uri($d)";

        String[] result = null;
        
        try {
            XQResultSequence rs = executeQuery(query);
            if (rs != null) {
                List<String> allDocs = new ArrayList<String>();
                while(rs.next()) {
                    String uri = rs.getItemAsString(null).trim();
                    String[] parts = uri.split("/");
                    String doc = parts[parts.length-1];
                    allDocs.add(doc);
                }
                freeResources(rs);
                if (allDocs.size() > 0) {
                    result = allDocs.toArray(new String[0]);
                }
            }
        } 
        catch(XQException e) {
            LOG.error("Something wrong while get parent element!!");
        }

        return result;
    }
    
    @Override
    public void freeResources(XQResultSequence rs) throws XQException {
        baseFreeResources(rs);
    }
    
    private String preprocess(String query) {

        StringBuffer processedQuery = new StringBuffer();
        
        while (query.length() > 0) {
            int idx = query.indexOf("doc(");
            if (idx != -1) {
                // everything until 'doc('' is ok.
                processedQuery.append(query.substring(0,idx + 5));
                // remove doc( and the ' or " character after it
                query = query.substring(idx + 5);
                // remove ') or "), to get document name
                int idx2 = query.indexOf(')') - 1;
                String document = query.substring(0, idx2);
                // replace 'document.xml' by 'database/document.xml'
                processedQuery.append(getDatabaseName()+"/"+document);
                query = query.substring(idx2);
            } 
            else  {
                idx = query.indexOf("document(");
                if (idx != -1) {
                    // everything until 'document('' is ok.
                    processedQuery.append(query.substring(0,idx + 10));
                    // remove document( and the ' or " character after it
                    query = query.substring(idx+10);
                    // remove ') or "), to get document name
                    int idx2 = query.indexOf(')') - 1;
                    String document = query.substring(0, idx2);
                    // replace 'document.xml' by 'database/document.xml'
                    processedQuery.append(getDatabaseName()+"/"+document);
                    query = query.substring(idx2);
                }
                else {
                    processedQuery.append(query);
                    break;
                }
            }
        }
        
        return processedQuery.toString();
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

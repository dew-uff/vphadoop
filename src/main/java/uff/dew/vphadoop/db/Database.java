package uff.dew.vphadoop.db;

import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

public interface Database {

    public XQResultSequence executeQuery(String query) throws XQException;
    
    public String executeQueryAsString(String query) throws XQException;
    
    public void createCollection(String collectionName) throws XQException;
    
    public void deleteCollection(String collectionName) throws XQException;
    
    public void loadFileInCollection(String collectionName, String filePath) throws XQException;
    
    public void createCollectionWithContent(String collectionName, String dirPath) throws XQException;
    
    /**
     * returns the cardinality of the xpath expression in the database
     * 
     * @param xpath
     * @param document
     * @param collection
     * 
     * @return the cardinality, -1 if error
     */
    public int getCardinality(String xpath, String document, String collection);
    
    public String getHost();
    
    public int getPort();
    
    public String getUsername();
    
    public String getPassword();
    
    public String getType();


}


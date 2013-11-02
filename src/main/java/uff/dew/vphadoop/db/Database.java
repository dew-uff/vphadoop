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
    
    public int getCardinality(String xpath, String document, String collection) throws XQException;
    
    public String getHost();
    
    public int getPort();
    
    public String getUsername();
    
    public String getPassword();


}


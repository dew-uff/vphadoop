package uff.dew.vphadoop.db;

import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

public interface Database {

    public XQResultSequence executeQuery(String query) throws XQException;
    
    public String executeQueryAsString(String query) throws XQException;
    
    public void createCollection(String collectionName) throws XQException;
    
    public void deleteCollection(String collectionName) throws XQException;
}


package uff.dew.vphadoop.db;

import java.util.Map;

import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

import uff.dew.vphadoop.catalog.Element;

/**
 * This interface is used to provide necessary behavior to the application, 
 * independent from the underneath SGBD being used. The application must not 
 * worry about specific details on how to execute some tasks. Those details 
 * will be encapsulated on classes that implement this interface. 
 * 
 * @author Gabriel Tessarolli
 *
 */
public interface Database {

    /**
     * Execute the XQuery and return a XQResultSequence containing the result
     * The caller must iterate over this sequence to get the whole result
     * 
     * @param query The XQuery to execute 
     * @return a XQResultSequence object containing the result
     * @throws XQException
     */
    public XQResultSequence executeQuery(String query) throws XQException;
    
    /**
     * Execute the XQuery and return the result as a String
     * ATTENTION! The result may be a very large string! 
     * 
     * @param query The XQuery to execute 
     * @return a String object containing the result
     * @throws XQException
     */
    public String executeQueryAsString(String query) throws XQException;
    
    /**
     * Create a empty collection in the database
     * 
     * @param collectionName The name of the collection to be created
     * @throws XQException
     */
    public void createCollection(String collectionName) throws XQException;
    
    /**
     * Delete the given collection from the database
     * 
     * @param collectionName The name of the collection to be deleted
     * @throws XQException
     */
    public void deleteCollection(String collectionName) throws XQException;
    
    /**
     * Add a XML file in the given collection of the database
     *     
     * @param collectionName The name of the collection to be updated
     * @param filePath The path of the file in the FS to be added
     * @throws XQException
     */
    public void loadFileInCollection(String collectionName, String filePath) throws XQException;
    
    /**
     * Create a collection in the database with the XML files from a directory in the FS.
     * 
     * @param collectionName The name of the collection to be created
     * @param dirPath The path of the directory in the FS containing the XML files to be added
     * @throws XQException
     */
    public void createCollectionWithContent(String collectionName, String dirPath) throws XQException;
    
    /**
     * Return the cardinality of the xpath expression in a given document of a 
     * collection from the database.
     * 
     * @param xpath The XPath expression to be checked
     * @param document The name of the document on which to check the cardinality
     * @param collection The name of the collection containing the document
     * 
     * @return the cardinality, -1 if error
     */
    public int getCardinality(String xpath, String document, String collection);
    
    /**
     * Return the hostname of the database server
     * 
     * @return The hostname
     */
    public String getHost();
    
    /**
     * Return the port being used in the server
     * 
     * @return The port
     */
    public int getPort();
    
    /**
     * Return the username used to connect to the database server
     * 
     * @return The username
     */
    public String getUsername();
    
    /**
     * Return the password used to connect to the database server
     * 
     * @return The password
     */
    public String getPassword();
    
    /**
     * Return the type of the database.
     * It is one of the static identifiers listed in DatabaseFactory
     * 
     * @return The type of the database
     */
    public String getType();

    /**
     * 
     */
    public Map<String, Element> getCatalog();
}


package uff.dew.vphadoop.db;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;

public class DatabaseFactory {
    
    public static final String CONFIG_FILE_HOST_ELEMENT = "serverName";
    public static final String CONFIG_FILE_PORT_ELEMENT = "portNumber";
    public static final String CONFIG_FILE_USERNAME_ELEMENT = "userName";
    public static final String CONFIG_FILE_PASSWORD_ELEMENT = "userPassword";
    public static final String CONFIG_FILE_TYPE_ELEMENT = "type";
    public static final String CONFIG_FILE_DATABASE_ELEMENT = "databaseName";
    
    public static final String TYPE_BASEX = "BASEX";
    public static final String TYPE_SEDNA = "SEDNA";
    
    public static Database createDatabase(InputStream fileStream) throws IOException {
        
        try
        {
            DocumentBuilderFactory b = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = b.newDocumentBuilder();
            Document doc = builder.parse(fileStream);
            
            Database database;
            
            if (doc.getElementsByTagName(CONFIG_FILE_TYPE_ELEMENT).item(0)
                    .getTextContent().equals(TYPE_BASEX)) {
                database = getBaseXDatabase(doc);
            } else {
                database = getSednaDatabase(doc);
            }
            
            return database;
        }
        catch(Exception e) {
            throw new IOException(e);
        }
    }

    private static Database getBaseXDatabase(Document doc) throws IOException {
        String host = doc.getElementsByTagName(CONFIG_FILE_HOST_ELEMENT).item(0)
                .getTextContent();
        
        String port = doc.getElementsByTagName(CONFIG_FILE_PORT_ELEMENT).item(0)
                .getTextContent();
        
        String user = doc.getElementsByTagName(CONFIG_FILE_USERNAME_ELEMENT).item(0)
                .getTextContent();
        
        String pass = doc.getElementsByTagName(CONFIG_FILE_PASSWORD_ELEMENT).item(0)
                .getTextContent();
        
        return new BaseXDatabase(host, Integer.parseInt(port), user, pass);
    }
    
    private static Database getSednaDatabase(Document doc) throws IOException {
        String host = doc.getElementsByTagName(CONFIG_FILE_HOST_ELEMENT).item(0)
                .getTextContent();
        
        String port = doc.getElementsByTagName(CONFIG_FILE_PORT_ELEMENT).item(0)
                .getTextContent();
        
        String user = doc.getElementsByTagName(CONFIG_FILE_USERNAME_ELEMENT).item(0)
                .getTextContent();
        
        String pass = doc.getElementsByTagName(CONFIG_FILE_PASSWORD_ELEMENT).item(0)
                .getTextContent();
        
        String database = doc.getElementsByTagName(CONFIG_FILE_DATABASE_ELEMENT).item(0)
                .getTextContent();
        
        return new SednaDatabase(host, Integer.parseInt(port), user, pass, database);
    }
}

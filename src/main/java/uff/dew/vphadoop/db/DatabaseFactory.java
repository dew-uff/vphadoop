package uff.dew.vphadoop.db;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;

public class DatabaseFactory {
    
    private static final String CONFIG_FILE_HOST_ELEMENT = "host";
    private static final String CONFIG_FILE_PORT_ELEMENT = "port";
    private static final String CONFIG_FILE_USERNAME_ELEMENT = "username";
    private static final String CONFIG_FILE_PASSWORD_ELEMENT = "password";
    private static final String CONFIG_FILE_TYPE_ELEMENT = "type";
    
    private static final String TYPE_BASEX = "BASEX";
    
    public static Database createDatabase(InputStream fileStream) throws IOException {
        
        try
        {
            String host, port, user, pass;
            
            DocumentBuilderFactory b = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = b.newDocumentBuilder();
            Document doc = builder.parse(fileStream);
            
            if (!doc.getElementsByTagName(CONFIG_FILE_TYPE_ELEMENT).item(0)
                    .getTextContent().equals(TYPE_BASEX)) {
                throw new Exception("Only BaseX supported");
            }
            
            host = doc.getElementsByTagName(CONFIG_FILE_HOST_ELEMENT).item(0)
                    .getTextContent();
            
            port = doc.getElementsByTagName(CONFIG_FILE_PORT_ELEMENT).item(0)
                    .getTextContent();
            
            user = doc.getElementsByTagName(CONFIG_FILE_USERNAME_ELEMENT).item(0)
                    .getTextContent();
            
            pass = doc.getElementsByTagName(CONFIG_FILE_PASSWORD_ELEMENT).item(0)
                    .getTextContent();
            
            Database database = new BaseXDatabase(host, Integer.parseInt(port), user, pass);
            
            return database;
        }
        catch(Exception e) {
            throw new IOException(e);
        }
    }
}

package uff.dew.vphadoop.db;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xquery.XQDataSource;

import net.xqj.basex.BaseXXQDataSource;

import org.w3c.dom.Document;

public class DataSourceFactory {
    
    private static final String CONFIG_FILE_HOST_ELEMENT = "host";
    private static final String CONFIG_FILE_PORT_ELEMENT = "port";
    private static final String CONFIG_FILE_USERNAME_ELEMENT = "username";
    private static final String CONFIG_FILE_PASSWORD_ELEMENT = "password";
    private static final String CONFIG_FILE_TYPE_ELEMENT = "type";
    
    private static final String TYPE_BASEX = "BASEX";
    
    public static XQDataSource createDataSource(InputStream fileStream) throws IOException {
        
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
            
            BaseXXQDataSource ds = new BaseXXQDataSource();
            ds.setServerName(host);
            ds.setPort(Integer.parseInt(port));
            ds.setUser(user);
            ds.setPassword(pass);
            
            return ds;
        }
        catch(Exception e) {
            throw new IOException(e);
        }
    }
}

package uff.dew.vphadoop.catalog;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.vphadoop.db.Database;


public class Catalog {
    
    public static final Log LOG = LogFactory.getLog(Catalog.class);

	private static Catalog _instance = null;
	
	private Database database = null;
	
	private boolean dbMode = false;
	
	private Map<String,Document> documents;
	private Map<String,Collection> collections;

	private Catalog() {
	    documents = new HashMap<String,Document>();
	    collections = new HashMap<String,Collection>();
	}

	public static Catalog get() {
		if (_instance == null) {
			_instance = new Catalog();
		}
		return _instance;
	}
	
	public void setDatabaseObject(Database database) {
		this.database = database;
	}

    public int getCardinality(String xpath, String documentName, String collectionName) {
        
        LOG.debug("xpath: " + xpath);
        
        int cardinality = 0;
        
        if (dbMode == true || (documents == null && collections == null)) {
            LOG.debug("Operating DB Mode!");
            if (database != null) {
                cardinality = database.getCardinality(xpath, documentName, collectionName);
            }
            else {
                LOG.error("Database object is null! Returning invalid cardinality!");
                return -1;
            }               
        }
        else {
            xpath = "/" + xpath;
            if (collectionName != null) {
                Collection c = collections.get(collectionName);
                if (c != null) {
                    cardinality = c.getCardinality(xpath, documentName);
                }
            }
            else {
                if (documentName != null) {
                    Document d = documents.get(documentName);
                    if (d != null) {
                        cardinality = d.getCardinality(xpath);
                    }
                }
            }
        } 

        return cardinality;
    }
	
	public void populateCatalogFromFile(InputStream is) {
	    
	    // drop any existing catalog information;
	    documents = null;
	    collections = null;
	    documents = new HashMap<String,Document>();
	    collections = new HashMap<String,Collection>();

        try {
			XMLInputFactory factory = XMLInputFactory.newInstance();
			XMLStreamReader stream = factory.createXMLStreamReader(is);
			
			while (stream.hasNext()) {
			    int type = stream.next();
			    
			    switch (type) {
			    case XMLStreamReader.START_ELEMENT:
			        
			        if (stream.getLocalName() == "document") {
			            
			            Document doc = new Document();
			            doc.readFromCatalogStream(stream);
                        documents.put(doc.getName(), doc);
			        }
			        else if (stream.getLocalName() == "collection") {
			            
			            Collection collection = new Collection();
			            collection.readFromCatalogStream(stream);
                        collections.put(collection.getName(), collection);
			        }
			        break;
			    }
			}

		} catch (Exception e) {
			LOG.error("Error parsing XML catalog! Won't use catalog!");
		}         
    }
    
    public void setDbMode(boolean mode) {
    	this.dbMode = mode;
    }
    
    public void createCatalogFromRawResources(String[] resources) throws Exception {
        
        // drop any existing catalog information;
        documents = null;
        collections = null;
        documents = new HashMap<String,Document>();
        collections = new HashMap<String,Collection>();

        for (String res : resources) {

            File resFile = new File(res);

            if (!resFile.exists()) {
                System.err.println("Resource \"" + res + "\" doesn't seem to exist. Skipping it");
                // if it doesn't point to anywhere, skip it
                continue;
            }
            
            if (resFile.isDirectory()) {
                Collection c = new Collection();
                c.readFromRawDirectory(resFile);
                collections.put(c.getName(), c);
            }
            else {
                Document d = new Document();
                d.readFromRawFile(resFile);
                documents.put(d.getName(), d);
            }
        }
    }
    
    public void saveCatalog(OutputStream os) throws Exception {
        if (collections == null && documents == null) {
            LOG.error("Can't save catalog! There was a problem creating it.");
            return;
        }
        
        StringBuilder content = new StringBuilder();
        content.append("<?xml version=\"1.0\" ?>\n");
        content.append("<catalog>\n");
        
        for (Document doc : documents.values()) {
            content.append(doc.getAsXml());
        }
        
        for (Collection collection : collections.values()) {
            content.append(collection.getAsXml());
        }
        content.append("</catalog>");
        os.write(content.toString().getBytes());
    }
}

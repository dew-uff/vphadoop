package uff.dew.vphadoop.catalog;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseException;
import uff.dew.vphadoop.db.DatabaseFactory;


public class Catalog {
    
    public static final Log LOG = LogFactory.getLog(Catalog.class);

	private static Catalog _instance = null;
	
	private Database database;
	
	private Map<String,Element> elementsByPathMap;
	private Map<Integer,Element> elementsByIdMap;
	
	private Catalog() {
	}

	public static Catalog get() {
		if (_instance == null) {
			_instance = new Catalog();
		}
		return _instance;
	}

	public void parseDbConfig(InputStream dbConfigIS) throws IOException {
	    database = DatabaseFactory.createDatabaseObject(dbConfigIS);
	}
	
    public void setConfiguration(Configuration conf) throws IOException {
        
        String dbConfigFile = conf.get(VPConst.DB_CONFIGFILE_PATH);
        String catalogFile = conf.get(VPConst.CATALOG_FILE_PATH);
        
        FileSystem dfs = FileSystem.get(conf);
        
        InputStream dbConfigIS = dfs.open(new Path(dbConfigFile));
        database = DatabaseFactory.createDatabaseObject(dbConfigIS);
        dbConfigIS.close();

        long start = System.currentTimeMillis();
        InputStream catalogIS = null; 
        try {
        	catalogIS = dfs.open(new Path(catalogFile));
        	LOG.debug("Found catalog file: " + catalogFile);
        	populateCatalogFromFile(catalogIS);
        }
        catch (FileNotFoundException e) {
        	LOG.debug("Catalog file didn't exist. Creating it...");
            createCatalog();
            saveCatalogFile(catalogFile);
        }
        LOG.debug("Finished catalog processing: " + (System.currentTimeMillis() - start) + "ms.");
    }
	
	private void saveCatalogFile(String catalogFile) {
        if (elementsByPathMap == null) {
        	LOG.error("Can't save catalog! There was a problem creating it.");
            return;
        }
        
        try {
            FileOutputStream fos = new FileOutputStream(catalogFile);
            StringBuilder content = new StringBuilder();
            content.append("<?xml version=\"1.0\" ?>\n");
            content.append("<catalog>\n");

            for (Element e: elementsByPathMap.values()) {
                content.append("<element id=\""+e.getId() +"\" name=\""+e.getName()+
                        "\" count=\""+e.getCount()+"\" path=\""+e.getPath()+"\" parent=\""+
                        (e.getParent()!=null?e.getParent().getId():-1)+"\"/>\n");
            }
            content.append("</catalog>");
            fos.write(content.toString().getBytes());
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

    private void createCatalog() {
	    
    	// get the map according to the specific database system
	    elementsByPathMap = database.getCatalog();
	    
	    if (elementsByPathMap != null) {
		    elementsByIdMap = new HashMap<Integer,Element>();
	    	for (Element e : elementsByPathMap.values()) {
	    		elementsByIdMap.put(new Integer(e.getId()), e);
	    	}	    	
	    }
    }

    private void populateCatalogFromFile(InputStream is) {
        
    	elementsByPathMap = new HashMap<String, Element>();
    	elementsByIdMap = new HashMap<Integer, Element>();
    	
        try {
			XMLInputFactory factory = XMLInputFactory.newInstance();
			XMLStreamReader stream = factory.createXMLStreamReader(is);
			
			while (stream.hasNext()) {
			    int type = stream.next();
			    
			    switch (type) {
			    case XMLStreamReader.START_ELEMENT:
			        
			    	if (stream.getLocalName() == "element") {
			        	
			        	String idString = stream.getAttributeValue(null, "id");
			            String name = stream.getAttributeValue(null, "name");
			            String countString = stream.getAttributeValue(null, "count");
			            String path = stream.getAttributeValue(null, "path");
			            String parentString = stream.getAttributeValue(null, "parent");
			            
			            if (idString!= null && countString != null 
			            		&& name != null && path != null 
			            		&& parentString != null) {

			            	int id = Integer.parseInt(idString);
			            	int cardinality = Integer.parseInt(countString);
			            	int parentId = Integer.parseInt(parentString);

			            	Element element = new Element(id, name, cardinality, path);
			            	element.setParentId(parentId);
			            	elementsByIdMap.put(new Integer(id), element);
			            	elementsByPathMap.put(path, element);
			            }                    
			        }
			        break;
			    }
			}
			
			// after getting all elements, still need to match parents nodes;
			for (Element e : elementsByIdMap.values()) {
				if (e.getParentId() != -1) {
					e.setParent(elementsByIdMap.get(new Integer(e.getParentId())));
				}
			}
		} catch (Exception e) {
			LOG.error("Error parsing XML catalog! Won't use catalog!");
	    	elementsByPathMap = null;
	    	elementsByIdMap = null;			
		}         
    }
    
    public int getCardinality(String xpath, String document, String collection) 
	        throws DatabaseException {
        
    	LOG.debug("xpath: " + xpath);
        
        int cardinality = 0;
        
        // if catalog exists, use it
        if (elementsByPathMap != null) {
        	Element element = elementsByPathMap.get("/"+xpath);
            if (element == null) {
            	cardinality = 0;
            }
            else {
            	cardinality = element.getCount();
            }    
        } 
        // otherwise proceed with the cardinality query
        else {
    	    if (database != null) {
    	    	cardinality = database.getCardinality(xpath, document, collection);
    	    }
    	    else {
    	    	throw new DatabaseException("Database object is null!");
    	    }        	
        }
        return cardinality;
	}
	
	public Database getDatabase() {
	    return database;
	}
}

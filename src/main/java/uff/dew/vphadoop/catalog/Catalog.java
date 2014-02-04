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

import uff.dew.vphadoop.db.Database;


public class Catalog {
    
    public static final Log LOG = LogFactory.getLog(Catalog.class);

	private static Catalog _instance = null;
	
	private Database database = null;
	
	private boolean dbMode = false;
	
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
	
	public void setDatabaseObject(Database database) {
		this.database = database;
	}
	
	public void saveCatalogToFile(String catalogFile) {
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

    public void createCatalog() {
	    
    	// get the map according to the specific database system
	    elementsByPathMap = database.getCatalog();
	    
	    if (elementsByPathMap != null) {
		    elementsByIdMap = new HashMap<Integer,Element>();
	    	for (Element e : elementsByPathMap.values()) {
	    		elementsByIdMap.put(new Integer(e.getId()), e);
	    	}	    	
	    }
    }

    public void populateCatalogFromFile(InputStream is) {
        
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
    
    public void setDbMode(boolean mode) {
    	this.dbMode = mode;
    }
    
    public int getCardinality(String xpath, String document, String collection) {
        
    	LOG.debug("xpath: " + xpath);
        
        int cardinality = 0;
        
        if (dbMode == true || elementsByPathMap == null) {
        	LOG.debug("Operating DB Mode!");
    	    if (database != null) {
    	    	cardinality = database.getCardinality(xpath, document, collection);
    	    }
    	    else {
    	    	LOG.error("Database object is null! Returning invalid cardinality!");
    	    	return -1;
    	    }             	
        }
        else {
        	Element element = elementsByPathMap.get("/"+xpath);
            if (element == null) {
            	cardinality = 0;
            }
            else {
            	cardinality = element.getCount();
            }    
        } 

        return cardinality;
	}
}

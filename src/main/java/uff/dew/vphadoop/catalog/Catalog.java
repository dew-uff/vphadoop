package uff.dew.vphadoop.catalog;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
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
	
	private Map<String,List<Element>> elementsByNameMap;
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
	
	public void parseCatalog(String catalogFilepath) throws IOException {
	    File catalogFile = new File(catalogFilepath);
	    if (catalogFile.exists()) {
	        InputStream catalogIS = new FileInputStream(catalogFile);
	        populateCatalogFromFile(catalogIS);
	        catalogIS.close();
	    }
        if (elementsByNameMap == null) {
            createCatalog();
            saveCatalogFile(catalogFilepath);
        }
	}
	
    public void setConfiguration(Configuration conf) throws IOException {
        
        String dbConfigFile = conf.get(VPConst.DB_CONFIGFILE_PATH);
        String catalogFile = conf.get(VPConst.CATALOG_FILE_PATH);
        
        FileSystem dfs = FileSystem.get(conf);
        
        InputStream dbConfigIS = dfs.open(new Path(dbConfigFile));
        database = DatabaseFactory.createDatabaseObject(dbConfigIS);
        dbConfigIS.close();

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
    }
	
	private void saveCatalogFile(String catalogFile) {
        if (elementsByNameMap == null) {
            return;
        }
        
        try {
            FileOutputStream fos = new FileOutputStream(catalogFile);
            StringBuilder content = new StringBuilder();
            content.append("<?xml version=\"1.0\" ?>\n");
            content.append("<catalog>\n");
            for (List<Element> elements : elementsByNameMap.values()) {
                for (Element e: elements) {
                    content.append("<element id=\""+e.getId() +"\" name=\""+e.getName()+
                            "\" count=\""+e.getCount()+"\" path=\""+e.getPath()+"\" parent=\""+
                            (e.getParent()!=null?e.getParent().getId():-1)+"\"/>\n");
                }
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
	    
	    elementsByNameMap = database.getCatalog();
	    
	    elementsByIdMap = new HashMap<Integer, Element>();
	    for(List<Element> sameNameElements : elementsByNameMap.values()) {
	    	for (Element e : sameNameElements) {
	    		elementsByIdMap.put(new Integer(e.getId()), e);
	    	}
	    }
    }

    private void populateCatalogFromFile(InputStream is) {
        
    	elementsByNameMap = new HashMap<String, List<Element>>();
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

			            	List<Element> elementsSameName = elementsByNameMap.get(name);
			            	if (elementsSameName == null) {
			            		elementsSameName = new ArrayList<Element>();
			            	}
			            	elementsSameName.add(element);
			            	elementsByNameMap.put(name, elementsSameName);
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
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FactoryConfigurationError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XMLStreamException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}            
    }
    
    public int getCardinality(String xpath, String document, String collection) 
	        throws DatabaseException {
        LOG.debug("xpath: " + xpath);
        
	    if (database != null) {
	        return database.getCardinality(xpath, document, collection);
	    }
	    else {
	        throw new DatabaseException("Database object is null!");
	    }
	}
	
	public Database getDatabase() {
	    return database;
	}
}

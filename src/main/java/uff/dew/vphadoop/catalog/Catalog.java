package uff.dew.vphadoop.catalog;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

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
	
	private Map<String,List<Element>> theCatalog;
	
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
        if (theCatalog == null) {
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
        	populateCatalogFromFile(catalogIS);
        }
        catch (FileNotFoundException e) {
            createCatalog();
            saveCatalogFile(catalogFile);
        }
    }
	
	private void saveCatalogFile(String catalogFile) {
        if (theCatalog == null) {
            return;
        }
        
        try {
            FileOutputStream fos = new FileOutputStream(catalogFile);
            StringBuilder content = new StringBuilder();
            content.append("<?xml version=\"1.0\" ?>\n");
            content.append("<catalog>\n");
            for (List<Element> elements : theCatalog.values()) {
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
	    
	    theCatalog = database.getCatalog();
    }

    private void populateCatalogFromFile(InputStream is) {
        
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

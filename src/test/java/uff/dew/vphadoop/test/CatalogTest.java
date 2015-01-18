package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.catalog.Catalog;
import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;

public class CatalogTest {
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        
        String[] resources = {"/home/gabriel/xmark/files/100MB/auction.xml"};
        
        Catalog c = Catalog.get();
        c.createCatalogFromRawResources(resources);
        FileOutputStream fos = new FileOutputStream("catalog-test.xml");
        c.saveCatalog(fos);
        fos.close();        
    }
    
    @Test
    public void readCatalogFromFile() throws Exception {
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        Catalog catalog = Catalog.get();
        catalog.setDatabaseObject(db);
        catalog.populateCatalogFromFile(new FileInputStream("catalog-test.xml"));
        assertEquals(25500,catalog.getCardinality("site/people/person", "auction.xml", null));
    }

}

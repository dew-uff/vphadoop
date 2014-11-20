package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;

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
        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONF_HOST, "127.0.0.1");
        conf.set(VPConst.DB_CONF_PORT, "1984");
        conf.set(VPConst.DB_CONF_USERNAME, "admin");
        conf.set(VPConst.DB_CONF_PASSWORD, "admin");
        conf.set(VPConst.DB_CONF_DATABASE, "test");
        conf.set(VPConst.DB_CONF_TYPE, "BASEX");
        DatabaseFactory.produceSingletonDatabaseObject(conf);
    }
    
    @Test
    public void testSaveCatalogFile() throws Exception {
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        
        Catalog catalog = Catalog.get();
        catalog.setDatabaseObject(db);
        catalog.createCatalog();
        catalog.saveCatalogToFile("catalog-test.xml");
    }
    
    @Test
    public void readCatalogFromFile() throws Exception {
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        Catalog catalog = Catalog.get();
        catalog.setDatabaseObject(db);
        catalog.populateCatalogFromFile(new FileInputStream("catalog-test.xml"));
        assertEquals(25500,catalog.getCardinality("site/people/person", "auction", null));
    }

}

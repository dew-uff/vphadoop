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

        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONF_HOST, "localhost");
        conf.set(VPConst.DB_CONF_PORT, "5050");
        conf.set(VPConst.DB_CONF_USERNAME, "SYSTEM");
        conf.set(VPConst.DB_CONF_PASSWORD, "MANAGER");
        conf.set(VPConst.DB_CONF_DATABASE, "10gb_md");
        conf.set(VPConst.DB_CONF_TYPE, "SEDNA");
        DatabaseFactory.produceSingletonDatabaseObject(conf);
    }
    
    @Test
    public void readCatalogFromFile() throws Exception {
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        Catalog catalog = Catalog.get();
        catalog.setDatabaseObject(db);
        catalog.populateCatalogFromFile(new FileInputStream("catalog-test.xml"));
        assertEquals(25500,catalog.getCardinality("site/people/person", "auction.xml", null));
    }

    @Test
    public void testCollection() throws Exception {
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        Catalog catalog = Catalog.get();
        catalog.setDatabaseObject(db);
        catalog.populateCatalogFromFile(new FileInputStream("/home/gabriel/xmark/catalogs/10GB_md/catalog.xml"));
        int cardinalityFromCatalog = catalog.getCardinality("site/people/person", null, "auctions");
        catalog.setDbMode(true);
        int cardinalityFromDb = catalog.getCardinality("site/people/person", null, "auctions");
        assertEquals(cardinalityFromCatalog,cardinalityFromDb,0);
    }
}

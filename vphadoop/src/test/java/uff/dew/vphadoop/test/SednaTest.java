package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.catalog.Element;
import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;

public class SednaTest {
    
    private static String TEST_COLLECTION_NAME = "testCollection";
    
    /**
     * Creates a db config to access the Sedna DB
     * 
     * IMPORTANT: The Sedna server should be up and running and there should be
     * a database there called "test", with a "auction" xml document from xmark,
     * generated with factor 1.0.
     * 
     * @throws Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONF_HOST, "127.0.0.1");
        conf.set(VPConst.DB_CONF_PORT, "5050");
        conf.set(VPConst.DB_CONF_USERNAME, "SYSTEM");
        conf.set(VPConst.DB_CONF_PASSWORD, "MANAGER");
        conf.set(VPConst.DB_CONF_DATABASE, "test");
        conf.set(VPConst.DB_CONF_TYPE, "SEDNA");
        DatabaseFactory.produceSingletonDatabaseObject(conf);
    }

    @Test
    public void testCreateCollection() {
        try {
            Database db = DatabaseFactory.getSingletonDatabaseObject();
            db.createCollection(TEST_COLLECTION_NAME);
            db.executeQueryAsString("collection('"+TEST_COLLECTION_NAME+"')");
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeleteCollection() {
        try {
            Database db = DatabaseFactory.getSingletonDatabaseObject();
            db.deleteCollection(TEST_COLLECTION_NAME);
        }
        catch (Exception e) {
            fail(e.getMessage());
        }
    }

//    @Test
//    public void testCreateCollectionWithContent() {
//        try {
//            Database db = DatabaseFactory.createDatabase(new FileInputStream(config));
//            String contentDir = XQueryBaseX.class.getResource("/testXml").getPath();
//            db.createCollectionWithContent(TEST_COLLECTION_NAME,contentDir);
//            db.executeQueryAsString("collection('"+TEST_COLLECTION_NAME+"')");
//        }
//        catch (Exception e) {
//            fail(e.getMessage());
//        }
//    }

    @Test
    public void testGetCardinality() {
        try {
            Database db = DatabaseFactory.getSingletonDatabaseObject();
            int cardinality = db.getCardinality(
                    "site/people/person", "auction", null);
            assertEquals(25500, cardinality);
            
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void testGetCatalog() {
        try {
            Database db = DatabaseFactory.getSingletonDatabaseObject();

            Map<String, Element> catalog = db.getCatalog();
            Element element = catalog.get("/site/people/person");
            
            assertEquals(element.getName(), "person");
            assertEquals(element.getPath(), "/site/people/person");
            assertEquals(element.getCount(), 25500);
                        
        } catch (Exception e) {
            fail(e.getMessage());
        }        
    }

//    @Test
//    public void testGetAncestral() {
//        try {
//            Database db = DatabaseFactory.createDatabase(new FileInputStream(
//                    config));
//            String parent = db.getAncestral("title", "dblp", null);
//            assertEquals("article", parent);
//        } catch (Exception e) {
//            fail(e.getMessage());
//        }
//    }
    
//    @Test
//    public void testLoadFileInCollection() {
//        fail("Not yet implemented");
//    }
//
}

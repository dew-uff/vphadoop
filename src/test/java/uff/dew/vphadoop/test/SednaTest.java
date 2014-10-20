package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.catalog.Element;
import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;

public class SednaTest {
    
    private static String TEST_COLLECTION_NAME = "testCollection";
    
    /**
     * Creates a db config file to access the Sedna DB
     * 
     * IMPORTANT: The Sedna server should be up and running and there should be
     * a database there called "test", with a "auction" xml document from xmark,
     * generated with factor 1.0.
     * 
     * @throws Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        File configFile = File.createTempFile("any", ".xml");
        FileWriter fw = new FileWriter(configFile);
        fw.write("<?xml version=\"1.0\"?> "
                + "<vphadoop> "
                + "<database> "
                + "<type>SEDNA</type> "
                + "<serverName>127.0.0.1</serverName> "
                + "<portNumber>5050</portNumber> "
                + "<userName>SYSTEM</userName> "
                + "<userPassword>MANAGER</userPassword> "
                + "<databaseName>test</databaseName>"
                + "</database> "
                + "</vphadoop>");
        fw.close();
        DatabaseFactory.produceSingletonDatabaseObject(new FileInputStream(
                configFile));
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

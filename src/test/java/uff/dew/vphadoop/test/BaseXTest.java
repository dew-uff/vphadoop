package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.catalog.Element;
import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;

public class BaseXTest {

    private static File config; 
    
    private static String TEST_COLLECTION_NAME = "testCollection";
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        config = File.createTempFile("any", ".xml");
        FileWriter fw = new FileWriter(config);
        fw.write("<?xml version=\"1.0\"?> "
                + "<vphadoop> "
                + "<database> "
                + "<type>BASEX</type> "
                + "<serverName>127.0.0.1</serverName> "
                + "<portNumber>1984</portNumber> "
                + "<userName>admin</userName> "
                + "<userPassword>admin</userPassword> "
                + "<databaseName>dblp</databaseName>"
                + "</database> "
                + "</vphadoop>");
        fw.close();
    }

//    @Test
//    public void testCreateCollection() {
//        try {
//            Database db = DatabaseFactory.createDatabase(new FileInputStream(config));
//            db.createCollection(TEST_COLLECTION_NAME);
//            db.executeQueryAsString("collection('"+TEST_COLLECTION_NAME+"')");
//        }
//        catch (Exception e) {
//            fail(e.getMessage());
//        }
//    }
//
//    @Test
//    public void testDeleteCollection() {
//        try {
//            Database db = DatabaseFactory.createDatabase(new FileInputStream(config));
//            db.deleteCollection(TEST_COLLECTION_NAME);
//        }
//        catch (Exception e) {
//            fail(e.getMessage());
//        }
//    }

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
            Database db = DatabaseFactory.createDatabase(new FileInputStream(
                    config));
            int cardinality = db.getCardinality("dblp/article", "dblp", null);
            assertEquals(643075, cardinality);
            
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    @Test
    public void testGetCatalog() {
        try {
            Database db = DatabaseFactory.createDatabase(new FileInputStream(
                    config));

            Map<String, List<Element>> catalog = db.getCatalog();
            List<Element> authorsElements = catalog.get("author");
            for(Element e: authorsElements) {
                System.out.println(e.getPath());
            }
            
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

package uff.dew.svp.db;

import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

import junit.framework.TestCase;
import uff.dew.svp.SvpTestQueries;

public class BaseXDatabaseTest extends TestCase {
    
    private static final String TEMP_COLLECTION = "temp";
    private static final String XML_FILES_DIR = "/home/gabriel/repo/xquery-mapreduce/svp-lib/test/partials/regular";
    private static final String XML_SINGLE_FILE = "/home/gabriel/repo/xquery-mapreduce/svp-lib/test/partials/aggregation/partial_sd_aggregation_000.xml";
    
    @Override
    protected void setUp() throws Exception {
        DatabaseFactory.produceSingletonDatabaseObject("localhost", 1984, "admin", "admin", "expdb", "BASEX");
    }
    
    @Override
    protected void tearDown() throws Exception {
        Database database = DatabaseFactory.getSingletonDatabaseObject();
        database.deleteCollection(TEMP_COLLECTION);
    }

    public void testExecuteQuery() {
        Database database = DatabaseFactory.getSingletonDatabaseObject();
        
        try {
            XQResultSequence res = database.executeQuery(SvpTestQueries.query_sd_regular);
            while (res.next()) {
                System.out.println(res.getItemAsString(null));
                System.out.println("\r\n");
            }
        } catch (XQException e) {
            e.printStackTrace();
            fail("wrong!");
        }
    }

    public void testExecuteQueryAsString() {
        Database database = DatabaseFactory.getSingletonDatabaseObject();

        try {
            System.out.println(database.executeQuery(SvpTestQueries.query_sd_regular));
            System.out.println("\r\n");
        } catch (XQException e) {
            e.printStackTrace();
            fail("wrong!");
        }
    }
        
    public void testCreateCollection() {
        Database database = DatabaseFactory.getSingletonDatabaseObject();
        try {
            database.createCollection(TEMP_COLLECTION);
        } catch (XQException e) {
            e.printStackTrace();
            fail("wrong");
        }
    }
    
    public void testDeleteCollection() {
        Database database = DatabaseFactory.getSingletonDatabaseObject();
        try {
            database.deleteCollection(TEMP_COLLECTION);
        } catch (XQException e) {
            e.printStackTrace();
            fail("wrong");
        }
    }



    public void testCreateCollectionWithContent() {
        Database database = DatabaseFactory.getSingletonDatabaseObject();
        try {
            database.createCollectionWithContent(TEMP_COLLECTION,XML_FILES_DIR);
        } catch (XQException e) {
            e.printStackTrace();
            fail("wrong");
        }
    }

    public void testGetCardinality() {
        Database database = DatabaseFactory.getSingletonDatabaseObject();
        int cardinality = database.getCardinality("/site/people/person", "auction.xml", null);
        assertEquals(25500, cardinality);
    }

    // TODO solve this
//    public void testLoadFileInCollection() {
//        Database database = DatabaseFactory.getSingletonDatabaseObject();
//        try {
//            database.loadFileInCollection(TEMP_COLLECTION,XML_SINGLE_FILE);
//        } catch (XQException e) {
//            e.printStackTrace();
//            fail("wrong");
//        }
//    }

    public void testGetParentElement() {
        Database database = DatabaseFactory.getSingletonDatabaseObject();
        String[] parent = database.getParentElement("person", null, "auction.xml");
        assertEquals("people", parent[0]);
        parent = database.getParentElement("results", TEMP_COLLECTION, "partial_sd_regular_000.xml");
        assertEquals("partialResult",parent[0]);
        parent = database.getParentElement("site", null, "auction.xml");
        assertEquals("",parent[0]);
    }

    public void testGetDocumentsNamesForCollection() {
        fail("Not yet implemented");
    }
}


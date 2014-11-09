package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;

import javax.xml.xquery.XQResultSequence;

import org.junit.Before;
import org.junit.Test;

import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.SednaDatabase;

public class TestLoadContentIntoCollection {

    private static final String TEST_COLLECTION = "collectionTeste";
    private static final String DOC1 = "<root><elm1><elm2>teste</elm2></elm1></root>";
    
    private Database db = null;
    
    @Before
    public void setup() {
        try {
            db = new SednaDatabase("localhost", 5050, "SYSTEM", "MANAGER", "xmark");
            db.deleteCollection(TEST_COLLECTION);
            
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testAddDocumentToCollection() {
        
        try {
            db.createCollection(TEST_COLLECTION);
            
            ByteArrayInputStream bais = new ByteArrayInputStream(DOC1.getBytes());
            db.addDocumentToCollection(bais, "doc1", TEST_COLLECTION);
            bais.close();
            
            XQResultSequence res = db.executeQuery("for $i in collection('"+TEST_COLLECTION+"')/root/elm1/elm2 return $i/text()");
            
            res.next();

            String item = res.getItemAsString(null);
            assertEquals("text { \"teste\" }", item);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

}

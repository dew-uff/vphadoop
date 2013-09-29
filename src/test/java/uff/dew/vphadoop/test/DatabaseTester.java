package uff.dew.vphadoop.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;

public class DatabaseTester {

    private File config; 
    
    @Before
    public void setUp() throws Exception {
        config = File.createTempFile("any", ".xml");
        FileWriter fw = new FileWriter(config);
        fw.write("<?xml version=\"1.0\"?> "
                + "<vphadoop> "
                + "<database> "
                + "<type>BASEX</type> "
                + "<host>127.0.0.1</host> "
                + "<port>1984</port> "
                + "<username>admin</username> "
                + "<password>admin</password> "
                + "</database> "
                + "</vphadoop>");
        fw.close();
    }

    @Test
    public void testCreateDatabaseObject() {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            Database ds = DatabaseFactory.createDatabase(fs.open(new Path(config.getAbsolutePath())));
            assertEquals("127.0.0.1", ds.getHost());
            assertEquals(1984, ds.getPort());
            assertEquals("admin", ds.getUsername());
            assertEquals("admin", ds.getPassword());
        } catch (Exception e) {
            fail("something is not right");
        }
        
    }
    
    @Test
    public void testCreateDeleteCollectionInDatabase() {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            Database ds = DatabaseFactory.createDatabase(fs.open(new Path(config.getAbsolutePath())));
            ds.createCollection("thisIsaTest");
            assertNotNull(ds.executeQuery("collection('thisIsaTest')"));
            ds.deleteCollection("thisIsaTest");
        } catch (Exception e) {
            fail("something is not right" + e.getMessage());
        }        
    }

}

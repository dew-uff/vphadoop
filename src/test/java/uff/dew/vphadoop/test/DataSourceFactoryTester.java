package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;

import javax.xml.xquery.XQDataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import uff.dew.vphadoop.db.DataSourceFactory;

public class DataSourceFactoryTester {

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
    public void testCreateDataSource() {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            XQDataSource ds = DataSourceFactory.createDataSource(fs.open(new Path(config.getAbsolutePath())));
            assertEquals("127.0.0.1", ds.getProperty("serverName"));
            assertEquals("1984", ds.getProperty("port"));
            assertEquals("admin", ds.getProperty("user"));
            assertEquals("admin", ds.getProperty("password"));
        } catch (Exception e) {
            fail("something is not right");
        }
        
    }

}

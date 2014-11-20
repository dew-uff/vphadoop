package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.VPConst;

public class DatabaseFactoryTest {
    
    private static String configurationFile = null;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        File configFile = File.createTempFile("any", ".xml");
        configurationFile = configFile.getAbsolutePath();
        FileWriter fw = new FileWriter(configFile);
        fw.write("<configuration>"
                + "<property><name>vphadoop.svp.numfragments</name><value>10</value></property>"   
                + "<property><name>vphadoop.svp.numfragments</name><value>10</value></property>"   
                +"<property><name>vphadoop.db.hostname</name><value>localhost</value></property>" 
                +"<property><name>vphadoop.db.port</name><value>5050</value></property>"  
                +"<property><name>vphadoop.db.username</name><value>SYSTEM</value></property>"  
                +"<property><name>vphadoop.db.password</name><value>MANAGER</value></property>"  
                +"<property><name>vphadoop.db.database</name><value>xmark</value></property>"  
                +"<property><name>vphadoop.db.type</name><value>SEDNA</value></property>"
                + "</configuration>");
        fw.close();
    }
    
    @Test
    public void testProduceSingletonDatabaseObject() {
        Configuration c = new Configuration();
        try {
            c.addResource(new FileInputStream(new File(configurationFile)));
        }
        catch (FileNotFoundException e) {
            fail("Conf nof found!");
        }
        c.reloadConfiguration();
        assertEquals("SEDNA", c.get(VPConst.DB_CONF_TYPE));
    }

    @Test
    public void testGetSingletonDatabaseObject() {
        fail("Not yet implemented");
    }

}

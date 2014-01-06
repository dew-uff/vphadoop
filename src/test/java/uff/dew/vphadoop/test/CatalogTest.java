package uff.dew.vphadoop.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.catalog.Catalog;

public class CatalogTest {
    
    private static File config;
    
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

    @Test
    public void testSaveCatalogFile() throws Exception {
        Catalog catalog = Catalog.get();
        catalog.parseDbConfig(new FileInputStream(config));
        catalog.parseCatalog("/home/hduser/catalog.xml");
    }
}

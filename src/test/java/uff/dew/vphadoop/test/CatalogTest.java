package uff.dew.vphadoop.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;

import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.catalog.Catalog;
import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;

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
                + "<databaseName>test</databaseName>"
                + "</database> "
                + "</vphadoop>");
        fw.close();
        DatabaseFactory.produceSingletonDatabaseObject(
                new FileInputStream(config));
    }
    
    

    @Test
    public void testSaveCatalogFile() throws Exception {
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        
        Catalog catalog = Catalog.get();
        catalog.setDatabaseObject(db);
        catalog.createCatalog();
        catalog.saveCatalogToFile("catalog-test.xml");
    }
    
    @Test
    public void readCatalogFromFile() throws Exception {
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        Catalog catalog = Catalog.get();
        catalog.setDatabaseObject(db);
        catalog.populateCatalogFromFile(new FileInputStream("catalog-test.xml"));
        assertEquals(25500,catalog.getCardinality("site/people/person", "auction", null));
    }

}

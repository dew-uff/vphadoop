package uff.dew.vphadoop.test;

import static org.junit.Assert.fail;

import org.junit.Test;

import uff.dew.vphadoop.catalog.Catalog;

public class CatalogGenerationTest {

    @Test
    public void testConstructFromFile() {
        
        String[] resources = {"/home/gabriel/xmark/files/10MB/auction.xml"};
        
        Catalog c = Catalog.get();
        try {
            c.createCatalogFromRawResources(resources);
            c.saveCatalog(System.out);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error during catalog creation from a single XML file.");
        }
    }
    
    @Test
    public void testConstructFromDirectory() {
        
        String[] resources = {"/home/gabriel/xmark/files/1GB_md"};
        
        Catalog c = Catalog.get();
        try {
            c.createCatalogFromRawResources(resources);
            c.saveCatalog(System.out);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error during catalog creation from a single XML file.");
        }
    }
}

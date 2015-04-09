package uff.dew.svp.catalog;

import java.io.File;

public class CatalogGenerator {
    
    public static void main(String[] args) {
        
        if (args.length < 1) {
            System.out.println("Usage: CatalogGenerator <resource> [<resource> ...]\n\n"
                    + "<resource> is a XML file or directory containing XML files");
            System.exit(0);
        }

        try {
            Catalog cg = Catalog.get();
            cg.createCatalogFromRawResources(args);
            cg.saveCatalog(System.out);
        }
        catch(Exception e) {
            System.err.println("Error during Catalog creation: " + e.getMessage());
            System.exit(1);
        }
    }
}

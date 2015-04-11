package uff.dew.svp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;

import junit.framework.TestCase;
import uff.dew.svp.db.DatabaseException;

public class FinalResultComposerTest extends TestCase {
    
    private final static String FINAL_RESULT_DIR = "/tmp/";

    private final static String DBHOST = "localhost";
    private final static int DBPORT = 5050;
    private final static String DBUSERNAME = "SYSTEM";
    private final static String DBPASSWORD = "MANAGER";
    private final static String DBTYPE = "SEDNA";
    private final static String DBNAME = "expdb";
    
    private final static String PARTIAL_RESULTS_DIRECTORY = "test/partials/";
    
    public void testExecuteFinalCompositionRegular() {
        
        try {
            FinalResultComposer frc = new FinalResultComposer();
            frc.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            frc.setExecutionContext(ExecutionContext.restoreFromStream(new FileInputStream("test/fragments/regular/frag_sd_regular_000.txt")));
            
            File partialsDir = new File(PARTIAL_RESULTS_DIRECTORY+"regular");
            File[] partials = partialsDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith("partial_sd_regular_") && name.endsWith(".xml")) {
                        return true;
                    }
                    return false;
                }
            });
            
            for (File partial : partials) {
                FileInputStream fis = new FileInputStream(partial);
                frc.loadPartial(fis);
                fis.close();
            }
            
            FileOutputStream fos = new FileOutputStream(FINAL_RESULT_DIR + "final_sd_regular.xml");
            frc.combinePartialResults(fos,true);
            fos.close();
        } catch (FileNotFoundException e) {
            fail("wrong!");
            e.printStackTrace();
        } catch (DatabaseException e) {
            fail("wrong!");
            e.printStackTrace();
        } catch (IOException e) {
            fail("wrong!");
            e.printStackTrace();
        }
    }
    
    public void testExecuteFinalCompositionAggregation() {
        
        try {
            FinalResultComposer frc = new FinalResultComposer();
            frc.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            frc.setExecutionContext(ExecutionContext.restoreFromStream(new FileInputStream("test/fragments/aggregation/frag_sd_aggregation_000.txt")));
            
            File partialsDir = new File(PARTIAL_RESULTS_DIRECTORY+"aggregation");
            File[] partials = partialsDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith("partial_sd_aggregation_") && name.endsWith(".xml")) {
                        return true;
                    }
                    return false;
                }
            });
            
            for (File partial : partials) {
                FileInputStream fis = new FileInputStream(partial);
                frc.loadPartial(fis);
                fis.close();
            }
            
            FileOutputStream fos = new FileOutputStream(FINAL_RESULT_DIR + "final_sd_aggregation.xml");
            frc.combinePartialResults(fos,true);
            fos.close();
        } catch (FileNotFoundException e) {
            fail("wrong!");
            e.printStackTrace();
        } catch (DatabaseException e) {
            fail("wrong!");
            e.printStackTrace();
        } catch (IOException e) {
            fail("wrong!");
            e.printStackTrace();
        }
    }
    
    
}

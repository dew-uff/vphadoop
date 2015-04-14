package uff.dew.svp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

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
            FileOutputStream fos = new FileOutputStream(FINAL_RESULT_DIR + "final_sd_regular.xml");
            
            FinalResultComposer frc = new FinalResultComposer(fos);
            frc.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            frc.setExecutionContext(ExecutionContext.restoreFromStream(new FileInputStream("test/fragments/regular/frag_sd_regular_000.txt")));
            // necessary
            frc.cleanup();
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
            
            Arrays.sort(partials);
            
            for (File partial : partials) {
                FileInputStream fis = new FileInputStream(partial);
                frc.loadPartial(fis);
                fis.close();
            }
            frc.combinePartialResults();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("wrong!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("wrong!");
        } catch (IOException e) {
            e.printStackTrace();
            fail("wrong!");
        }
    }

    public void testExecuteFinalCompositionRegularForceTempCollectionMode() {
        
        try {
            FileOutputStream fos = new FileOutputStream(FINAL_RESULT_DIR + "final_sd_regular_temp_collection.xml");
            
            FinalResultComposer frc = new FinalResultComposer(fos);
            frc.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            frc.setForceTempCollectionExecutionMode(true);
            frc.setExecutionContext(ExecutionContext.restoreFromStream(new FileInputStream("test/fragments/regular/frag_sd_regular_000.txt")));
            // necessary
            frc.cleanup();
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
            frc.combinePartialResults();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("wrong!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("wrong!");
        } catch (IOException e) {
            e.printStackTrace();
            fail("wrong!");
        }
    }

    public void testExecuteFinalCompositionAggregation() {
        
        try {
            FileOutputStream fos = new FileOutputStream(FINAL_RESULT_DIR + "final_sd_aggregation.xml");
            FinalResultComposer frc = new FinalResultComposer(fos);
            frc.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            frc.setExecutionContext(ExecutionContext.restoreFromStream(new FileInputStream("test/fragments/aggregation/frag_sd_aggregation_000.txt")));
            // necessary!
            frc.cleanup();            
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
            
            frc.combinePartialResults();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("wrong!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("wrong!");
        } catch (IOException e) {
            e.printStackTrace();
            fail("wrong!");
        }
    }
}

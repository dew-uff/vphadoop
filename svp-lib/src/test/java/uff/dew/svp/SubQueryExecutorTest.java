package uff.dew.svp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.TestCase;
import uff.dew.svp.db.DatabaseException;

public class SubQueryExecutorTest extends TestCase {

    private final static String DBHOST = "localhost";
    private final static int DBPORT = 5050;
    private final static String DBUSERNAME = "SYSTEM";
    private final static String DBPASSWORD = "MANAGER";
    private final static String DBTYPE = "SEDNA";
    private final static String DBNAME = "expdb";
    
    private final static String PARTIAL_RESULTS_DIRECTORY = "/tmp/";
    private final static String FRAGMENTS_DIRECTORY = "test/fragments/";
    
    private static final String fragment_sd_regular = "<ORDERBY></ORDERBY>\n"
            + "<ORDERBYTYPE></ORDERBYTYPE>\n"
            + "<AGRFUNC>{}</AGRFUNC>#\n"
            + "<results> {\n"
            + "    for $p in doc('auction.xml')/site/people/person[position() >= 1 and position() < 2551]\n"
            + "    let $e := $p/homepage\n"
            + "    where count($e) = 0\n"
            + "    return\n"
            + "        <people_without_homepage>\n"
            + "            {$p/name}\n"
            + "        </people_without_homepage>} </results>\n";
    
    private static final String fragment_sd_orderby = "<ORDERBY>$pe/name</ORDERBY>\n"
            + "<ORDERBYTYPE></ORDERBYTYPE>\n"
            + "<AGRFUNC>{}</AGRFUNC>#\n"
            + "<results> {\n"
            + "    for $pe in doc('auction.xml')/site/people/person[position() >= 1 and position() < 2551]\n"
            + "    let $int := $pe/profile/interest\n"
            + "    where $pe/profile/business = \"Yes\" and count($int) > 1 \n"
            + "    order by $pe/name\n"
            + "    return\n"
            + "    <people>\n"
            + "        {$pe}\n"
            + "    </people>\n"
            + "} </results>\n";
    
    private static final String fragment_sd_join = "<ORDERBY></ORDERBY>\n"
            + "<ORDERBYTYPE></ORDERBYTYPE>\n"
            + "<AGRFUNC></AGRFUNC>#\n"
            + "<results> {\n"
            + "   for $it in doc('auction.xml')/site/regions/africa/item[position() >= 1 and position() < 56]\n"
            + "   for $co in doc('auction.xml')/site/closed_auctions/closed_auction\n"
            + "   where $co/itemref/@item = $it/@id   and $it/payment = \"Cash\"\n"
            + "    return\n"
            + "     <itens>\n"
            + "      {$co/price}\n"
            + "      {$co/date}\n"
            + "      {$co/quantity}\n"
            + "      {$co/type}\n"
            + "      {$it/payment}\n"
            + "      {$it/location}\n"
            + "      {$it/from}\n"
            + "      {$it/to}\n"
            + "    </itens> }</results>";
    
    private static final String fragment_sd_aggregation = "<ORDERBY></ORDERBY>\n"
            + "<ORDERBYTYPE></ORDERBYTYPE>\n"
            + "<AGRFUNC>{count($p)=count($p):summary/cont, average($p)=average($p):summary/media}</AGRFUNC>#\n"
            + "<results> {\n"
            + "    let $p := doc('auction.xml')/site/closed_auctions/closed_auction[position() >= 1 and position() < 976]\n"
            + "    return\n"
            + "        <summary>\n"
            + "            <cont>{count($p)}</cont>\n"
            + "            <media>{avg($p/price)}</media>\n"
            + "        </summary>} </results>";
    

    private static final String fragment_sd_orderby_descending = "<ORDERBY>$pe/name</ORDERBY>\n"
            + "<ORDERBYTYPE>descending</ORDERBYTYPE>\n"
            + "<AGRFUNC>{}</AGRFUNC>#\n"
            + "<results> {\n"
            + "    for $pe in doc('auction.xml')/site/people/person[position() >= 1 and position() < 2551]\n"
            + "    let $int := $pe/profile/interest\n"
            + "    where $pe/profile/business = \"Yes\" and count($int) > 1\n"
            + "    order by $pe/name descending\n"
            + "    return\n"
            + "    <people>\n"
            + "        {$pe}\n"
            + "    </people>} </results>";
    
    public SubQueryExecutorTest(String name) {
        super(name);
    }

    public void testExecuteSubQueryRegular() {
        FileOutputStream fos = null;
        try {
            SubQueryExecutor sqe = new SubQueryExecutor(fragment_sd_regular);
            sqe.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            fos = new FileOutputStream(PARTIAL_RESULTS_DIRECTORY + "partial_sd_regular.xml");
            sqe.executeQuery(fos);
        } catch (SubQueryExecutionException
                | DatabaseException | IOException e) {
            e.printStackTrace();
            fail("should execute fine");
        }
        finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    fail("things are not good, my friend!");
                }
            }
        }
    }
    
    public void testExecuteSubQueryOrderBy() {
        FileOutputStream fos = null;
        try {
            SubQueryExecutor sqe = new SubQueryExecutor(fragment_sd_orderby);
            sqe.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            fos = new FileOutputStream(PARTIAL_RESULTS_DIRECTORY + "partial_sd_orderby.xml");
            sqe.executeQuery(fos);
        } catch (SubQueryExecutionException
                | DatabaseException | IOException e) {
            e.printStackTrace();
            fail("should execute fine");
        }
        finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    fail("things are not good, my friend!");
                }
            }
        }
    }

    public void testExecuteSubQueryOrderByDescending() {
        FileOutputStream fos = null;
        try {
            SubQueryExecutor sqe = new SubQueryExecutor(fragment_sd_orderby_descending);
            sqe.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            fos = new FileOutputStream(PARTIAL_RESULTS_DIRECTORY + "partial_sd_orderby_descending.xml");
            sqe.executeQuery(fos);
        } catch (SubQueryExecutionException
                | DatabaseException | IOException e) {
            e.printStackTrace();
            fail("should execute fine");
        }
        finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    fail("things are not good, my friend!");
                }
            }
        }
    }
    
    public void testExecuteSubQueryJoin() {
        FileOutputStream fos = null;
        try {
            SubQueryExecutor sqe = new SubQueryExecutor(fragment_sd_join);
            sqe.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            fos = new FileOutputStream(PARTIAL_RESULTS_DIRECTORY + "partial_sd_join.xml");
            sqe.executeQuery(fos);
        } catch (SubQueryExecutionException
                | DatabaseException | IOException e) {
            e.printStackTrace();
            fail("should execute fine");
        }
        finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    fail("things are not good, my friend!");
                }
            }
        }
    }
    
    public void testExecuteSubQueryAggregation() {
        FileOutputStream fos = null;
        try {
            SubQueryExecutor sqe = new SubQueryExecutor(fragment_sd_aggregation);
            sqe.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
            fos = new FileOutputStream(PARTIAL_RESULTS_DIRECTORY + "partial_sd_aggregation.xml");
            sqe.executeQuery(fos);
        } catch (SubQueryExecutionException
                | DatabaseException | IOException e) {
            e.printStackTrace();
            fail("should execute fine");
        }
        finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    fail("things are not good, my friend!");
                }
            }
        }
    }
    
    public void generatePartialsForFinalComposerTestRegular() {
        FileOutputStream fos = null;
        try {
            File testDir = new File(FRAGMENTS_DIRECTORY + "regular");
            File[] fragfiles = testDir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File f) {
                    if (f.getName().startsWith("frag_sd_regular_")) {
                        return true;
                    }
                    return false;
                }
            });
            for (int i = 0; i < fragfiles.length; i++) {
                String fragment = readContentFromFile(fragfiles[i]);
                SubQueryExecutor sqe = new SubQueryExecutor(fragment);
                sqe.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
                String partialFilename = PARTIAL_RESULTS_DIRECTORY + "partial_sd_regular_"+String.format("%1$03d", i)+".xml";
                fos = new FileOutputStream(partialFilename);
                boolean hasResult = sqe.executeQuery(fos);
                fos.flush();
                fos.close();
                fos = null;
                if (!hasResult) {
                    File f = new File(partialFilename);
                    f.delete();
                }
            }
        } catch (SubQueryExecutionException
                | DatabaseException | IOException e) {
            e.printStackTrace();
            fail("should execute fine");
        }
        finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    fail("things are not good, my friend!");
                }
            }
        }
    }
    
    public void generatePartialsForFinalComposerTestAggregation() {
        FileOutputStream fos = null;
        try {
            File testDir = new File(FRAGMENTS_DIRECTORY + "aggregation");
            File[] fragfiles = testDir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File f) {
                    if (f.getName().startsWith("frag_sd_aggregation_")) {
                        return true;
                    }
                    return false;
                }
            });
            for (int i = 0; i < fragfiles.length; i++) {
                String fragment = readContentFromFile(fragfiles[i]);
                SubQueryExecutor sqe = new SubQueryExecutor(fragment);
                sqe.setDatabaseInfo(DBHOST, DBPORT, DBUSERNAME, DBPASSWORD, DBNAME, DBTYPE);
                String partialFilename = PARTIAL_RESULTS_DIRECTORY + "partial_sd_aggregation_"+String.format("%1$03d", i)+".xml";
                fos = new FileOutputStream(partialFilename);
                boolean hasResult = sqe.executeQuery(fos);
                fos.flush();
                fos.close();
                fos = null;
                if (!hasResult) {
                    File f = new File(partialFilename);
                    f.delete();
                }
            }
        } catch (SubQueryExecutionException
                | DatabaseException | IOException e) {
            e.printStackTrace();
            fail("should execute fine");
        }
        finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    fail("things are not good, my friend!");
                }
            }
        }
    }

    /**
     * Load the file content into a String object
     * 
     * @param filename The file
     * @return the content of the file in a string object
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static String readContentFromFile(File file) throws FileNotFoundException, IOException {
        
        BufferedReader br = new BufferedReader(new FileReader(file));
        String everything = null;
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append('\n');
                line = br.readLine();
            }
            everything = sb.toString().trim();
        } finally {
            br.close();
        }
        
        return everything;
    }
}

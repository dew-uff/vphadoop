package uff.dew.svp;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import uff.dew.svp.db.DatabaseException;
import uff.dew.svp.exceptions.PartitioningException;

/**
 * Unit test for Partitioner.
 */
public class PartitionerTest extends TestCase
{
    private final static String DBHOST = "localhost";
    private final static int DBPORT = 5050;
    private final static String DBUSERNAME = "SYSTEM";
    private final static String DBPASSWORD = "MANAGER";
    private final static String DBTYPE = "SEDNA";
    private final static String DBNAME = "expdb";
    private final static String CATALOG_FILE = "/home/gabriel/xmark/catalogs/100MB/catalog.xml";
    private final static String FRAGMENTS_DIRECTORY = "/tmp/";

    
    private final static String query_sd_regular = "<results> {"
            + "    for $p in doc('auction.xml')/site/people/person"
            + "    let $e := $p/homepage"
            + "    where count($e) = 0"
            + "    return"
            + "        <people_without_homepage>"
            + "            {$p/name}"
            + "        </people_without_homepage>"
            + "} </results>";
    
    private final static String query_sd_order_by = "<results> {"
            + "    for $pe in doc('auction.xml')/site/people/person "
            + "    let $int := $pe/profile/interest "
            + "    where $pe/profile/business = \"Yes\" and count($int) > 1"
                    + "    order by $pe/name"
                    + "    return"
                    + "    <people>"
                    + "        {$pe}"
                    + "    </people>"
                    + "} </results>";

    private final static String query_sd_order_by_descending = "<results> {"
            + "    for $pe in doc('auction.xml')/site/people/person "
            + "    let $int := $pe/profile/interest "
            + "    where $pe/profile/business = \"Yes\" and count($int) > 1"
                    + "    order by $pe/name descending"
                    + "    return"
                    + "    <people>"
                    + "        {$pe}"
                    + "    </people>"
                    + "} </results>";
    
    private final static String query_sd_join = "<results> {   "
            + "for $it in doc('auction.xml')/site/regions/africa/item   "
            + "for $co in doc('auction.xml')/site/closed_auctions/closed_auction   "
            + "where $co/itemref/@item = $it/@id   and $it/payment = \"Cash\"    "
                    + "return     <itens>"
                    + "      {$co/price}"
                    + "      {$co/date}"
                    + "      {$co/quantity}"
                    + "      {$co/type}"
                    + "      {$it/payment}"
                    + "      {$it/location}"
                    + "      {$it/from}"
                    + "      {$it/to}"
                    + "    </itens>"
                    + " }</results>";
    
    private final static String query_sd_aggregation = "<results> {"
            + "    let $p := doc('auction.xml')/site/closed_auctions/closed_auction"
            + "    return"
            + "        <summary>"
            + "            <cont>{count($p)}</cont>"
            + "            <media>{avg($p/price)}</media>"
            + "        </summary>"
            + "} </results>";

    private static final String query_sd_incomplete_path = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in document('auction.xml')//person \r\n"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    private static final String query_sd_incomplete_path_join = 
            "<results> \r\n"
            + "{ \r\n"
            + "  for $p in document('auction.xml')//person \r\n"
            + "  for $a in document('auction.xml')//closed_auction \r\n"
            + "  where $p/@id = $a/@person \r\n"
            + "      return \r\n"
            + "        <buyer> \r\n"
            + "          {$p/name} \r\n"
            + "          {$a/price} \r\n"
            + "        </buyer> \r\n"
            + "    } </results>";
    
    private static final String query_md_regular = 
            "<results> {\n"
            + "for $op in collection('md')/site/open_auctions/open_auction\n"
            + "let $bd := $op/bidder where count($op/bidder) > 5\n"
            + "return\n"
            + "<open_auctions_with_more_than_5_bidders>\n"
            +   "<auction>\n"
            +       "{$op}\n"
            +   "</auction>\n"
            +   "<qty_bidder>\n"
            +   "{count($op/bidder)}\n"
            +   "</qty_bidder>\n"
            + "</open_auctions_with_more_than_5_bidders>\n"
            +"} </results>";
    
    private static final String query_md_incomplete_path = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in collection('auctions')//person \r\n"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    private static final String query_md_incomplete_path_join = 
            "<results> \r\n"
            + "{ \r\n"
            + "  for $p in collection('auctions')//person \r\n"
            + "  for $a in collection('auctions')//closed_auction \r\n"
            + "  where $p/@id = $a/@person \r\n"
            + "      return \r\n"
            + "        <buyer> \r\n"
            + "          {$p/name} \r\n"
            + "          {$a/price} \r\n"
            + "        </buyer> \r\n"
            + "    } </results>";
    
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PartitionerTest(String testName)
    {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite(PartitionerTest.class);
    }


    public void testPartitionerSDRegular()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_regular, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_regular.txt", fragments.get(0));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
    
    public void testPartitionerSDRegularDbMode()
    {
        try{
            Partitioner partitioner = new Partitioner(DBHOST,DBPORT,DBUSERNAME,DBPASSWORD,DBNAME,DBTYPE);
            List<String> fragments = partitioner.executePartitioning(query_sd_regular, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_regular_dbmode.txt", fragments.get(0));
            
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("something wrong during execution!");
        }
    }
    
    public void testPartitionerSDOrderBy()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_order_by, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_orderby.txt", fragments.get(0));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
    
    public void testPartitionerSDOrderByDbMode()
    {
        try{
            Partitioner partitioner = new Partitioner(DBHOST,DBPORT,DBUSERNAME,DBPASSWORD,DBNAME,DBTYPE);
            List<String> fragments = partitioner.executePartitioning(query_sd_order_by, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_orderby_dbmode.txt", fragments.get(0));
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("something wrong during execution!");
        }
    }
    
    public void testPartitionerSDOrderByDescending()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_order_by_descending, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_orderby_descending.txt", fragments.get(0));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
    
    public void testPartitionerSDOrderByDescendingDbMode()
    {
        try{
            Partitioner partitioner = new Partitioner(DBHOST,DBPORT,DBUSERNAME,DBPASSWORD,DBNAME,DBTYPE);
            List<String> fragments = partitioner.executePartitioning(query_sd_order_by_descending, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_orderby_descending_dbmode.txt", fragments.get(0));
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("something wrong during execution!");
        }
    }
    
    public void testPartitionerSDJoin()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_join, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_join.txt", fragments.get(0));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
    
    public void testPartitionerSDJoinDbMode()
    {
        try{
            Partitioner partitioner = new Partitioner(DBHOST,DBPORT,DBUSERNAME,DBPASSWORD,DBNAME,DBTYPE);
            List<String> fragments = partitioner.executePartitioning(query_sd_join, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_join_dbmode.txt", fragments.get(0));
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("something wrong during execution!");
        }
    }
    
    public void testPartitionerSDAggregation()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_aggregation, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_aggregation.txt", fragments.get(0));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
    
    public void testPartitionerSDAggregationDbMode()
    {
        try{
            Partitioner partitioner = new Partitioner(DBHOST,DBPORT,DBUSERNAME,DBPASSWORD,DBNAME,DBTYPE);
            List<String> fragments = partitioner.executePartitioning(query_sd_aggregation, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_aggregation_dbmode.txt", fragments.get(0));
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("something wrong during execution!");
        }
    }
    
    public void testPartitionerSDIncompletePath()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_incomplete_path, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_incomp_path.txt", fragments.get(0));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
    
    public void testPartitionerSDIncompletePathDbMode()
    {
        try{
            Partitioner partitioner = new Partitioner(DBHOST,DBPORT,DBUSERNAME,DBPASSWORD,DBNAME,DBTYPE);
            List<String> fragments = partitioner.executePartitioning(query_sd_incomplete_path, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_incomp_path_dbmode.txt", fragments.get(0));
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("something wrong during execution!");
        }
    }
    
    public void testPartitionerSDIncompletePathJoin()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_incomplete_path_join, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_incomp_path_join.txt", fragments.get(0));
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
    
    public void testPartitionerSDIncompletePathJoinDbMode()
    {
        try{
            Partitioner partitioner = new Partitioner(DBHOST,DBPORT,DBUSERNAME,DBPASSWORD,DBNAME,DBTYPE);
            List<String> fragments = partitioner.executePartitioning(query_sd_incomplete_path_join, 10);
            saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_incomp_path_join_dbmode.txt", fragments.get(0));
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        } catch (DatabaseException e) {
            e.printStackTrace();
            fail("something wrong during execution!");
        }
    }
    
    private void saveToFile(String filename,String content) {
        try {
            FileOutputStream fos = new FileOutputStream(filename);
            fos.write(content.getBytes());
            fos.write('\n');
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void generateTestBaselineForFinalResultComposer()
    {
        FileInputStream catalogStream = null;
        try{
            catalogStream = new FileInputStream(CATALOG_FILE);
            Partitioner partitioner = new Partitioner(catalogStream);
            List<String> fragments = partitioner.executePartitioning(query_sd_regular, 10);
            for (int i = 0; i < fragments.size(); i++) {
                saveToFile(FRAGMENTS_DIRECTORY + "frag_sd_regular_"+ String.format("%1$03d", i) +".txt", fragments.get(i));
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            fail("should find catalog!");
        } catch (PartitioningException e) {
            e.printStackTrace();
            fail("should partition fine!");
        }
    }
}

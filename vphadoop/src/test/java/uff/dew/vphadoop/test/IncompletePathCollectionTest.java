package uff.dew.vphadoop.test;

import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.util.ArrayList;

import mediadorxml.engine.XQueryEngine;
import mediadorxml.fragmentacaoVirtualSimples.ExistsJoinOperation;
import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.fragmentacaoVirtualSimples.SimpleVirtualPartitioning;
import mediadorxml.fragmentacaoVirtualSimples.SubQuery;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.catalog.Catalog;
import uff.dew.vphadoop.db.DatabaseFactory;

public class IncompletePathCollectionTest {
    
    private static final String QUERY_WITH_INCOMPLETE_PATH = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in collection('auctions')//person \r\n"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    private static final String QUERY_WITH_JOIN_COLLECTION_AND_INCOMPLETE_PATH = 
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
    
    
    private static final String QUERY = QUERY_WITH_INCOMPLETE_PATH;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONF_HOST, "localhost");
        conf.set(VPConst.DB_CONF_PORT, "5050");
        conf.set(VPConst.DB_CONF_USERNAME, "SYSTEM");
        conf.set(VPConst.DB_CONF_PASSWORD, "MANAGER");
        conf.set(VPConst.DB_CONF_DATABASE, "10gb_md");
        conf.set(VPConst.DB_CONF_TYPE, "SEDNA");
        DatabaseFactory.produceSingletonDatabaseObject(conf);
    }

    @Test
    public void testWithDbMode() {
        try {
            Catalog.get().setDatabaseObject(DatabaseFactory.getSingletonDatabaseObject());
            Catalog.get().setDbMode(true);
            
            ArrayList<String> docQueries = null;
            ArrayList<String> docQueriesWithoutFragmentation = null;
            String originalQuery = null;
            String query = QUERY;
            
            Query q = Query.getUniqueInstance(true);
            q.setInputQuery(query);
            docQueries = q.setqueryExprType(query);
            if ( docQueries!=null && docQueries.size() > 0 ){
                docQueriesWithoutFragmentation = docQueries;
            }
            
            SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(false);
            if (q.getqueryExprType()!=null && q.getqueryExprType().equals("collection")){
                if (docQueries!=null){
                    originalQuery = query;
                    for (String docQry : docQueries) {
                        query = docQry;
                        ExistsJoinOperation ej = new ExistsJoinOperation(query);
                        ej.verifyInputQuery();
                        q.setLastReadCardinality(-1);
                        q.setJoinCheckingFinished(false);  
                        
                        q.setOrderBy("");
                        XQueryEngine engine = new XQueryEngine();
                        engine.execute(originalQuery);
                        
                        if (q.getPartitioningPath()!=null && !q.getPartitioningPath().equals("")) {
                            SubQuery sbq = SubQuery.getUniqueInstance(false); 
                            SimpleVirtualPartitioning svp1 = new SimpleVirtualPartitioning();
                            svp1.setCardinalityOfElement(q.getLastCollectionCardinality());
                            svp1.setNumberOfNodes(3);                       
                            svp1.getSelectionPredicateToCollection(q.getVirtualPartitioningVariable(), q.getPartitioningPath(), query);                                         
                            q.setAddedPredicate(true);
                        }
                    }
                    
                    query = originalQuery;
                }
            }
            
            SubQuery sbq = SubQuery.getUniqueInstance(true);
            ArrayList<String> fragments = sbq.getSubQueries();
                    
            int i = 0;
            for(String f : fragments) {
                System.out.println("Fragment " + i++ + ": " + f);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
    @Test
    public void testWithCatalog() {
        try {
            Catalog.get().setDbMode(false);
            Catalog.get().populateCatalogFromFile(new FileInputStream("/home/gabriel/xmark/catalogs/10GB_md/catalog.xml"));
            
            ArrayList<String> docQueries = null;
            ArrayList<String> docQueriesWithoutFragmentation = null;
            String originalQuery = null;
            String query = QUERY;
            
            Query q = Query.getUniqueInstance(true);
            q.setInputQuery(query);
            docQueries = q.setqueryExprType(query);
            if ( docQueries!=null && docQueries.size() > 0 ){
                docQueriesWithoutFragmentation = docQueries;
            }
            
            SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(false);
            if (q.getqueryExprType()!=null && q.getqueryExprType().equals("collection")){
                if (docQueries!=null){
                    originalQuery = query;
                    for (String docQry : docQueries) {
                        query = docQry;
                        ExistsJoinOperation ej = new ExistsJoinOperation(query);
                        ej.verifyInputQuery();
                        q.setLastReadCardinality(-1);
                        q.setJoinCheckingFinished(false);  
                        
                        q.setOrderBy("");
                        XQueryEngine engine = new XQueryEngine();
                        engine.execute(originalQuery);
                        
                        if (q.getPartitioningPath()!=null && !q.getPartitioningPath().equals("")) {
                            SubQuery sbq = SubQuery.getUniqueInstance(false); 
                            SimpleVirtualPartitioning svp1 = new SimpleVirtualPartitioning();
                            svp1.setCardinalityOfElement(q.getLastCollectionCardinality());
                            svp1.setNumberOfNodes(3);                       
                            svp1.getSelectionPredicateToCollection(q.getVirtualPartitioningVariable(), q.getPartitioningPath(), query);                                         
                            q.setAddedPredicate(true);
                        }
                    }
                    
                    query = originalQuery;
                }
            }
            
            SubQuery sbq = SubQuery.getUniqueInstance(true);
            ArrayList<String> fragments = sbq.getSubQueries();
                    
            int i = 0;
            for(String f : fragments) {
                System.out.println("Fragment " + i++ + ": " + f);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}

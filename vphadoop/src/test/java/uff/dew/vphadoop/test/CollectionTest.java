package uff.dew.vphadoop.test;

import static org.junit.Assert.fail;

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

public class CollectionTest {
    
    private static final String QUERY_12 = 
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
    
    private static final String QUERY_14_ORIG = "<results>\n"
            + "{\n"
            + "for $it in collection('samericaItens')/orders/itens/item\n"
            + "for $pe in collection('samericaItens')/orders/people/person\n"
            + "where $pe/profile/interest/@category =\n"
            + "$it/incategory/@category\n"
            + "return\n"
            + "<people>\n"
            + "{$pe}\n"
            + "</people>\n"
            + "} </results>";
    
    private static final String QUERY = QUERY_12;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONF_HOST, "127.0.0.1");
        conf.set(VPConst.DB_CONF_PORT, "5050");
        conf.set(VPConst.DB_CONF_USERNAME, "SYSTEM");
        conf.set(VPConst.DB_CONF_PASSWORD, "MANAGER");
        conf.set(VPConst.DB_CONF_DATABASE, "xmark");
        conf.set(VPConst.DB_CONF_TYPE, "SEDNA");
        DatabaseFactory.produceSingletonDatabaseObject(conf);
        Catalog.get().setDatabaseObject(DatabaseFactory.getSingletonDatabaseObject());
    }
    
    @Test
    public void testExecuteString() {
        try {
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

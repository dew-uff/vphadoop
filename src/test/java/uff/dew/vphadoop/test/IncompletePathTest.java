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

public class IncompletePathTest {
    
    private static final String QUERY_WITH_INCOMPLETE_PATH = 
            " <results> \r\n" +
            " { \r\n"+
            "   for $p in doc('auction.xml')//person \r\n"+
            " return \r\n"+
            "  <person> \r\n"+
            "    {$p/name} \r\n"+
            "  </person> \r\n"+
            " } \r\n"+ 
            " </results>"; 
    
    private static final String QUERY = QUERY_WITH_INCOMPLETE_PATH;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONF_HOST, "localhost");
        conf.set(VPConst.DB_CONF_PORT, "5050");
        conf.set(VPConst.DB_CONF_USERNAME, "SYSTEM");
        conf.set(VPConst.DB_CONF_PASSWORD, "MANAGER");
        conf.set(VPConst.DB_CONF_DATABASE, "xmark");
        conf.set(VPConst.DB_CONF_TYPE, "SEDNA");
        DatabaseFactory.produceSingletonDatabaseObject(conf);
    }

    @Test
    public void testWithDbMode() {
        try {
            Catalog.get().setDatabaseObject(DatabaseFactory.getSingletonDatabaseObject());
            Catalog.get().setDbMode(true);
            
            String query = QUERY;
            
            Query q = Query.getUniqueInstance(true);
            q.setInputQuery(query);
            
            SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(false);
            svp.setNumberOfNodes(3);
            svp.setNewDocQuery(true);                                                       
            ExistsJoinOperation ej = new ExistsJoinOperation(QUERY);
            ej.verifyInputQuery();
            q.setLastReadCardinality(-1);
            q.setJoinCheckingFinished(false);               
            
            if ( q.getqueryExprType()!= null && !q.getqueryExprType().contains("collection") ) { // se a consulta de entrada n�o cont�m collection, execute a fragmenta��o virtual.
            
                XQueryEngine engine = new XQueryEngine();
                engine.execute(QUERY, false); // Para debugar o parser, passe o segundo par�metro como true.               
                
                q.setJoinCheckingFinished(true);
                
                if (q.isExistsJoin()){
                    q.setOrderBy("");                       
                    engine.execute(QUERY, false); // Executa pela segunda vez, por�m desta vez fragmenta apenas um dos joins
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
            Catalog.get().populateCatalogFromFile(new FileInputStream("/home/gabriel/xmark/catalogs/100MB/catalog.xml"));
            
            String query = QUERY;
            
            Query q = Query.getUniqueInstance(true);
            q.setInputQuery(query);
            
            SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(false);
            svp.setNumberOfNodes(3);
            svp.setNewDocQuery(true);                                                       
            ExistsJoinOperation ej = new ExistsJoinOperation(QUERY);
            ej.verifyInputQuery();
            q.setLastReadCardinality(-1);
            q.setJoinCheckingFinished(false);               
            
            if ( q.getqueryExprType()!= null && !q.getqueryExprType().contains("collection") ) { // se a consulta de entrada n�o cont�m collection, execute a fragmenta��o virtual.
            
                XQueryEngine engine = new XQueryEngine();
                engine.execute(QUERY, false); // Para debugar o parser, passe o segundo par�metro como true.               
                
                q.setJoinCheckingFinished(true);
                
                if (q.isExistsJoin()){
                    q.setOrderBy("");                       
                    engine.execute(QUERY, false); // Executa pela segunda vez, por�m desta vez fragmenta apenas um dos joins
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

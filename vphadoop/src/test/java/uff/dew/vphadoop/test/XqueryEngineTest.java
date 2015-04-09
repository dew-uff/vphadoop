package uff.dew.vphadoop.test;

import static org.junit.Assert.fail;
import mediadorxml.engine.XQueryEngine;
import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.fragmentacaoVirtualSimples.SimpleVirtualPartitioning;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.db.DatabaseFactory;

public class XqueryEngineTest {
    
    private static final String QUERY = 
            "<results> { "
            + "for $it in doc('standard')/site/regions/africa/item "
            + "for $co in doc('standard')/site/closed_auctions/closed_auction "
            + "where $co/itemref/@item = $it/@id and $it/payment = \"Cash\" "
            + "return "
            + "<itens> "
            + "{$co/price} "
            + "{$co/date} "
            + "{$co/quantity} "
            + "{$co/type} "
            + "{$it/payment} "
            + "{$it/location} "
            + "{$it/from} "
            + "{$it/to} "
            + "</itens> } "
            + "</results>";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Configuration conf = new Configuration();
        conf.set(VPConst.DB_CONF_HOST, "127.0.0.1");
        conf.set(VPConst.DB_CONF_PORT, "1984");
        conf.set(VPConst.DB_CONF_USERNAME, "admin");
        conf.set(VPConst.DB_CONF_PASSWORD, "admin");
        conf.set(VPConst.DB_CONF_DATABASE, "test");
        conf.set(VPConst.DB_CONF_TYPE, "BASEX");
        DatabaseFactory.produceSingletonDatabaseObject(conf);
        SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(true);
        svp.setNumberOfNodes(3);
        svp.setNewDocQuery(true); 
        Query q = Query.getUniqueInstance(true);
        q.setInputQuery(QUERY);
    }
    
    @Test
    public void testExecuteString() {
        XQueryEngine engine = new XQueryEngine();
        try {
            engine.execute(QUERY);
        } 
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}

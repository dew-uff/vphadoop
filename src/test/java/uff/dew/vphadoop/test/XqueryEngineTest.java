package uff.dew.vphadoop.test;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import mediadorxml.engine.XQueryEngine;
import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.fragmentacaoVirtualSimples.SimpleVirtualPartitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.catalog.Catalog;

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
    public static void setUpBeforeClass() {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://hadoop-dev:9000/");
        conf.set("mapred.job.tracker", "hadoop-dev:9001");
        try {
            writeDbConfiguration(conf);
            conf.set(VPConst.DB_CONFIGFILE_PATH, "configuration.xml");
            Catalog.get().setConfiguration(conf);
            SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(true);
            svp.setNumberOfNodes(3);
            svp.setNewDocQuery(true); 
            Query q = Query.getUniqueInstance(true);
            q.setInputQuery(QUERY);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
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
    
    private static void writeDbConfiguration(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create("vphadoop"), conf);
        FSDataOutputStream out = fs.create(new Path("configuration.xml"),true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        bw.write("<?xml version=\"1.0\"?>\n");
        bw.write("<vphadoop>\n");
        bw.write("<database>\n");
        bw.write("<type>"+ "BASEX" +"</type>\n");
        bw.write("<host>127.0.0.1</host>\n");
        bw.write("<port>1984</port>\n");
        bw.write("<username>admin</username>\n");
        bw.write("<password>admin</password>\n");
        bw.write("</database>\n");
        bw.write("</vphadoop>\n");
        bw.close();
        out.close();
    }

}

package uff.dew.vphadoop.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.vphadoop.Catalog;
import uff.dew.vphadoop.XmlDBConst;
import uff.dew.vphadoop.xquery.XPathExpression;

public class VPInputFormat extends InputFormat<IntWritable, Text> {
    
    public static final Log LOG = LogFactory.getLog(VPInputFormat.class
            .getName());

    @Override
    public RecordReader<IntWritable, Text> createRecordReader(InputSplit in,
            TaskAttemptContext ctxt) throws IOException, InterruptedException {

        return new VPRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext ctxt) throws IOException,
            InterruptedException {

        Configuration conf = ctxt.getConfiguration();
        Catalog.get().setConfiguration(conf);
        String xquery = conf.get(XmlDBConst.DB_XQUERY);
        String doc = conf.get(XmlDBConst.DB_DOCUMENT);
        
        List<InputSplit> splits = new ArrayList<InputSplit>();

        XPathExpression xpe = new XPathExpression(doc,xquery);
        
        // determine partition attribute
        int cardinality;
        int level = 0;
        while ((cardinality = Catalog.get().getCardinality(xpe.getSubPath(level))) == 1) {
            level++;
        }
        LOG.debug("Cardinality: " + cardinality);
        int step = 1000;
        int begin = 0;

        while (begin < cardinality) {
            int length = (cardinality - begin > step) ? step : cardinality
                    - begin;
            InputSplit range = new VPInputSplit(begin, length, level);
            splits.add(range);
            begin += step;
        }
        LOG.debug("# of splits: " + splits.size());
        
        return splits;
    }
}

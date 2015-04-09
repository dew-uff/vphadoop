package uff.dew.vphadoop.connector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.svp.Partitioner;
import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;
import uff.dew.vphadoop.VPConst;

public class VPInputFormat extends InputFormat<IntWritable, Text> {
    
    private static final Log LOG = LogFactory.getLog(VPInputFormat.class);
    private String inputQuery;
    private int nsplits = 10;
    private int nrecords = 5;
    private int nfragments = 50;

    @Override
    public RecordReader<IntWritable, Text> createRecordReader(InputSplit in,
            TaskAttemptContext ctxt) throws IOException, InterruptedException {

        return new VPRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext ctxt) throws IOException,
            InterruptedException {

        List<InputSplit> splits = null;
        
        Configuration conf = ctxt.getConfiguration();
        
        // the query to process
        inputQuery = conf.get(VPConst.XQUERY);
        nsplits = conf.getInt(VPConst.SVP_NUM_SPLITS, 10);
        nrecords = conf.getInt(VPConst.SVP_RECORDS_PER_SPLIT, 5);
        
        nfragments = nsplits * nrecords;
        
        try {
            long start = System.currentTimeMillis();
            String catalogPath = conf.get(VPConst.CATALOG_FILE_PATH);
            
            Partitioner partitioner = null;
            if (catalogPath != null && catalogPath.length() > 0) {
                LOG.debug("partitioner catalog mode!");
                FileInputStream catalogStream = new FileInputStream(catalogPath);
                partitioner = new Partitioner(catalogStream);
            } else {
                LOG.debug("partitioner database mode!");
                partitioner = createPartitionerDbMode(conf);
            }

            List<String> queries = partitioner.executePartitioning(inputQuery,
                    nfragments);
            
            // this is to avoid getting consecutive intervals processed in the same machine.
            // when we allocate a split to be processed in a task, if there is more than one 
            // record in each split, in terms of load balancing, that would be the same as having
            // a bigger split with all records merged. if there were a significant amount of data
            // in this interval, this processor would take more time, no matter what.
            Collections.shuffle(queries);

            long partitionTime = System.currentTimeMillis() - start;
            LOG.info("VP:partitioningTime: " + partitionTime + "ms");
            
            splits = new ArrayList<InputSplit>();

            int qcount = 0;
            
            for (int i = 0; i < nsplits; i++) {
                List<String> qs = new ArrayList<>(nrecords);
                for (int j = 0; j < nrecords; j++) {
                    qs.add(queries.get(qcount));
                    LOG.trace("Split["+i+"]Record["+j+"] = Queries["+qcount+"] = " + queries.get(qcount));
                    qcount++;
                }
                int initialPos = Integer.parseInt(SubQuery.getIntervalBeginning(queries.get(0)));
                InputSplit is = new VPInputSplit(initialPos, qs);
                splits.add(is);
            }
            
            FileSystem fs = FileSystem.get(conf);
            Path p = new Path("hack.txt");
            OutputStream os = fs.create(p,true);
            partitioner.getExecutionContext().save(os);
            os.close();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }

        LOG.debug("# of splits: " + splits.size());
        
        return splits;
    }

    private Partitioner createPartitionerDbMode(Configuration conf) throws Exception {
        String type = conf.get(VPConst.DB_CONF_TYPE);
        if (type == null) {
            throw new Exception("SGBDX type not specified in job configuration file");
        }
        
        String hostname = conf.get(VPConst.DB_CONF_HOST);
        if (hostname == null) {
            throw new Exception("SGBDX host not specified in job configuration file");
        }
        
        String portstr = conf.get(VPConst.DB_CONF_PORT);
        if (portstr == null) {
            throw new Exception("SGBDX port not specified in job configuration file");
        }
        int port = -1;
        try {
            port = Integer.parseInt(portstr);
        }
        catch (NumberFormatException e) {
            throw new Exception("SGBDX port specified is not a number");
        }
        
        String username = conf.get(VPConst.DB_CONF_USERNAME);
        if (username == null) {
            throw new Exception("SGBDX username not specified in job configuration file");
        }

        String password = conf.get(VPConst.DB_CONF_PASSWORD);
        if (password == null) {
            throw new Exception("SGBDX password not specified in job configuration file");
        }

        String database = conf.get(VPConst.DB_CONF_DATABASE);
        if (database == null) {
            throw new Exception("SGBDX database not specified in job configuration file");
        }

        return new Partitioner(hostname, port, username, password, database, type);
    }
}

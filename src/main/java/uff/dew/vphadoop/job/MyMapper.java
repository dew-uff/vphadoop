package uff.dew.vphadoop.job;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import uff.dew.svp.SubQueryExecutionException;
import uff.dew.svp.SubQueryExecutor;
import uff.dew.svp.db.DatabaseException;
import uff.dew.vphadoop.VPConst;

public class MyMapper extends Mapper<IntWritable, Text, NullWritable, Text> {

    private static final Log LOG = LogFactory.getLog(MyMapper.class);
    
    public static final String PARTIALS_DIR = "partials";
    
    public MyMapper() {

    }

    @Override
    protected void map(IntWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        
        long start = System.currentTimeMillis();
        
        // value is the fragment file from SVP
        String fragment = value.toString(); 
        
        Configuration conf = context.getConfiguration();
        OutputStream out = null;
        
        try {
            SubQueryExecutor sqe = new SubQueryExecutor(fragment);

            sqe.setDatabaseInfo(conf.get(VPConst.DB_CONF_HOST), conf.getInt(VPConst.DB_CONF_PORT,5050), 
                conf.get(VPConst.DB_CONF_USERNAME), conf.get(VPConst.DB_CONF_PASSWORD), 
                conf.get(VPConst.DB_CONF_DATABASE), conf.get(VPConst.DB_CONF_TYPE));

            // execute query, saving result to a partial file in hdfs
            FileSystem fs = FileSystem.get(context.getConfiguration());
            
            boolean zip = context.getConfiguration().getBoolean(VPConst.COMPRESS_DATA, true);

            String filename = null;
            Path filepath = null;
            
            if (zip) {
                filename = PARTIALS_DIR + "/partial_" + String.format("%1$020d", key.get()) + "_" + context.getTaskAttemptID() + ".zip";
                filepath = new Path(filename);
                OutputStream zipFile = fs.create(filepath);
        
                ZipOutputStream zipout = new ZipOutputStream(new BufferedOutputStream(zipFile));
                ZipEntry entry = new ZipEntry("partial_" + String.format("%1$020d", key.get()) + ".xml");
                zipout.putNextEntry(entry);
                out = zipout;
            }
            else {
                filename = PARTIALS_DIR + "/partial_" + String.format("%1$020d", key.get()) + "_" + context.getTaskAttemptID() + ".xml";
                filepath = new Path(filename);
                out = fs.create(filepath);
            }

            boolean hasResults = sqe.executeQuery(out);

            out.flush();
            out.close();
            out = null;
            
            // if it doesn't have results, delete the partial file
            if (!hasResults) {
                fs.delete(filepath, false);
            }
            
            long timeProcessing = System.currentTimeMillis() - start;
            incrementNodeProcessingTime(timeProcessing, context);
            LOG.debug("VP:mapper:executionTime: " + timeProcessing + " ms.");
            
            if (hasResults) {
                context.getCounter(VPCounters.PARTITIONS_WITH_RESULT).increment(1);
                // reducer will receive a list of filenames containing the fragments
                context.write(NullWritable.get(), new Text(filename));
            }
        }
        catch (DatabaseException e) {
            LOG.error("Something wrong with the database configuration", e);
            throw new IOException(e);
        }
        catch (SubQueryExecutionException e) {
            LOG.error("Something wrong with the query execution", e);
            throw new IOException(e);
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

	private void incrementNodeProcessingTime(long timeProcessing, Context context) {
    	// get name of the node
		String hostname;
		try {
			hostname = InetAddress.getLocalHost().getHostName();
			// TODO
			if (hostname.equals("XPS14")) {
	    		context.getCounter(VPCounters.NODE0_TOTAL_TIME).increment(timeProcessing);
	    	}			
			
		} catch (UnknownHostException e) {
			LOG.warn("Could not determint node name. Won't log time per node!");
		}
	}
}

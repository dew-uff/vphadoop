package uff.dew.vphadoop.connector;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.vphadoop.catalog.Catalog;



public class VPRecordReader extends RecordReader<IntWritable, Text> {
    
    public static final Log LOG = LogFactory.getLog(RecordReader.class);
    
    // flags to indicate current position
    private static final int NOT_STARTED = -1;
    private static final int DONE = -2;
	
	private int current = NOT_STARTED;
	private int startPos = 0;
	
	private String xquery = null;
	
	public VPRecordReader() {
	    LOG.trace("CONSTRUCTOR() " + this);
	}
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException,
			InterruptedException {
//	    if (current == NOT_STARTED || current == DONE) {
//	        return null;
//	    }
//		return new IntWritable(current);
	    return new IntWritable(startPos);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
//	    try {
	        //XMLStreamReader reader = rs.getItemAsStream();
			//return new Text(reader.getText());
	        //return new Text(rs.getItemAsString(null));
	        return new Text(xquery);
//		} catch (XQException e) {
//			throw new IOException(e);
//		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (current == NOT_STARTED) return 0;
	    if (current == DONE) return 1;
	    //return (current-first+1)/(float)total;
	    return 1;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext ctxt)
			throws IOException, InterruptedException {
		
	    readConfiguration(ctxt.getConfiguration());
		
		VPInputSplit vpSplit = (VPInputSplit) split;
		
		xquery = vpSplit.getFragmentQuery();
		startPos = vpSplit.getStartPosition();
//		total = vpSplit.getLengh();
//		selectionLevel = vpSplit.getSelectionLevel();
		
		LOG.debug("xquery: " + xquery);
        //LOG.trace("initialize() from " + first + " to " + (first + total - 1));
		
		//parallelProcessingInit();
		
		//readData();
	}

	private void readConfiguration(Configuration conf) throws IOException {

	    // the subquery for this fragment
	    //xquery = conf.get(VPConst.DB_XQUERY);
	    
	    // configure catalog for this task tracker
		Catalog.get().setConfiguration(conf);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
        if (current == DONE) {
            return false;
        }
        current = DONE;
        return true;
	}
}

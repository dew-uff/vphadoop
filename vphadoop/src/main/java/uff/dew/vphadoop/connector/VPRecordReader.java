package uff.dew.vphadoop.connector;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;



public class VPRecordReader extends RecordReader<IntWritable, Text> {
    
    public static final Log LOG = LogFactory.getLog(RecordReader.class);
    
    // flags to indicate current position
    private static final int NOT_STARTED = -1;
    private static final int DONE = -2;
	
	private int currentKey = NOT_STARTED;
	private String currentValue = null;
	
	Iterator<String> iterator = null;
	long counter = 0;
	long size = 0;
	
	public VPRecordReader() {
	    LOG.trace("CONSTRUCTOR() " + this);
	}
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException,
			InterruptedException {
	    if (currentKey == NOT_STARTED || currentKey == DONE) {
	        return null;
	    }
		return new IntWritable(currentKey);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
	    if (currentValue != null) {
	        return new Text(currentValue);
	    }
	    else {
	        return null;
	    }
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (currentKey == NOT_STARTED) return 0;
	    if (currentKey == DONE) return 1;
	    
	    return counter/size;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext ctxt)
			throws IOException, InterruptedException {
		
		VPInputSplit vpSplit = (VPInputSplit) split;
		
		iterator = vpSplit.getQueriesIterator();
		size = vpSplit.getLength();
		counter = 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
        if (iterator.hasNext()) {
            currentValue = iterator.next();
            currentKey = Integer.parseInt(SubQuery.getIntervalBeginning(currentValue));
            counter++;
            return true;
        } 
        else {
            currentValue = null;
            currentKey = DONE;
            return false;
        }
	}
}

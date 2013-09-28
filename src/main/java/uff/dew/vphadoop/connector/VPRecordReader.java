package uff.dew.vphadoop.connector;
import java.io.IOException;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.db.DataSourceFactory;
import uff.dew.vphadoop.xquery.XPathExpression;



public class VPRecordReader extends RecordReader<IntWritable, Text> {
    
    public static final Log LOG = LogFactory.getLog(RecordReader.class);
    
    // flags to indicate current position
    private static final int NOT_STARTED = -1;
    private static final int DONE = -2;

	//private int first = 0;
	//private int total = 0;
	//private int selectionLevel = 0;
	
	private int current = NOT_STARTED;
	
	private XQDataSource xqs = null;
	private XQResultSequence rs = null;
	
	private String xquery = null;
	private StringBuilder result = null;
	
	public VPRecordReader() {
	    LOG.trace("CONSTRUCTOR() " + this);
	}
	
	@Override
	public void close() throws IOException {
	    LOG.trace("close() " + this);
	    try {
			rs.close();
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public IntWritable getCurrentKey() throws IOException,
			InterruptedException {
//	    if (current == NOT_STARTED || current == DONE) {
//	        return null;
//	    }
//		return new IntWritable(current);
	    return new IntWritable(1);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
	    LOG.debug("getCurrentValue() = "+result.toString());
//	    try {
	        //XMLStreamReader reader = rs.getItemAsStream();
			//return new Text(reader.getText());
	        //return new Text(rs.getItemAsString(null));
	        return new Text(result.toString());
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
		
		xquery = vpSplit.getFragment();
		
//		first = vpSplit.getStartPosition();
//		total = vpSplit.getLengh();
//		selectionLevel = vpSplit.getSelectionLevel();
		
		LOG.debug("xquery: " + xquery);
        //LOG.trace("initialize() from " + first + " to " + (first + total - 1));
		
		readData();
	}

	private void readConfiguration(Configuration conf) throws IOException {

	    xquery = conf.get(VPConst.DB_XQUERY);
	    
		String configFile = conf.get(VPConst.DB_CONFIGFILE_PATH);
		FileSystem dfs = FileSystem.get(conf);
		
		xqs = DataSourceFactory.createDataSource(dfs.open(new Path(configFile)));
	}

	private void readData() throws IOException {
	    
	    //XPathExpression xpe = new XPathExpression(xquery);
	    //String selection = "position()=(" + first + " to " + (first+total-1) + ")";
	    //String finalExpr = xpe.getPathWithSelection(selection, selectionLevel);
	    String finalExpr = xquery.split("#")[1];
	    LOG.debug("readData() xquery: " + finalExpr);
	    try {
	        long startTimestamp = System.currentTimeMillis();
			XQConnection conn = xqs.getConnection();
			XQPreparedExpression xqpe = conn.prepareExpression(finalExpr);        
			rs = xqpe.executeQuery();
			LOG.debug("readData() query executed in " + 
			        (System.currentTimeMillis()-startTimestamp) +" ms.");
			
			result = new StringBuilder();
			while (rs.next()) {
                result.append(rs.getItemAsString(null));
            }
			
			rs.close();
			conn.close();
		} catch (XQException e) {
			throw new IOException(e);
		}
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

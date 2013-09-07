package uff.dew.vphadoop.connector;
import java.io.IOException;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import net.xqj.basex.BaseXXQDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.vphadoop.XmlDBConst;



public class VPRecordReader extends RecordReader<IntWritable, Text> {
    
    public static final Log LOG = LogFactory.getLog(RecordReader.class
            .getName());
    
    private static final int NOT_STARTED = -1;
    private static final int DONE = -2;

	private int first = 0;
	private int total = 0;
	
	private int current = -1;
	
	private XQDataSource xqs = null;
	private XQResultSequence rs = null;
	
	private String user = null;
	private String pass = null;
	private String doc = null;
	private String xquery = null;
	
	public VPRecordReader() {
	    LOG.debug("CONSTRUCTOR() " + this);
	}
	
	@Override
	public void close() throws IOException {
	    LOG.debug("close() " + this);
	    try {
			rs.close();
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public IntWritable getCurrentKey() throws IOException,
			InterruptedException {
	    if (current == NOT_STARTED || current == DONE) {
	        return null;
	    }
		return new IntWritable(current);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
	    try {
			return new Text(rs.getItem().getItemAsString(null));
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (current == NOT_STARTED) return 0;
	    if (current == DONE) return 1;
	    return (current-first+1)/(float)total;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext ctxt)
			throws IOException, InterruptedException {
		
	    setupDbClient(ctxt);
		
		VPInputSplit vpSplit = (VPInputSplit) split;
		
		first = vpSplit.getStartPosition();
		total = vpSplit.getLengh();
		
        LOG.debug("initialize() from " + first + " to " + (first + total - 1));
		
		readData();
	}

	private void setupDbClient(TaskAttemptContext ctxt) {
		
	    Configuration conf = ctxt.getConfiguration();
		String server = conf.get(XmlDBConst.DB_HOST);
		String port = conf.get(XmlDBConst.DB_PORT);
		user = conf.get(XmlDBConst.DB_USER);
		pass = conf.get(XmlDBConst.DB_PASSWORD);
		doc = conf.get(XmlDBConst.DB_DOCUMENT);
		xquery = conf.get(XmlDBConst.DB_XQUERY);
		
		try {
			xqs = new BaseXXQDataSource();
			xqs.setProperty("serverName", server);
			xqs.setProperty("port", port);
		} catch (XQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void readData() throws IOException {
	    
	    String xquery = new String("doc('"+doc+"')"+"/site/people/person"+"[position()=("+first+" to "+(first+total-1)+")]/name/text()");
	    LOG.debug("readData() xquery: " + xquery);
	    try {
			XQConnection conn = xqs.getConnection(user, pass);
			XQPreparedExpression xqpe =
			//conn.prepareExpression("doc('"+doc+"')"+xquery+"[position()=("+first+" to "+(first+total-1)+")]");
			        conn.prepareExpression(xquery);        
			rs = xqpe.executeQuery();
			
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
	    
	    try {
	        boolean result = rs.next();
	        if (result) {
	            current = (current == -1)?first:current + 1;
	        } else {
	            current = -2;
	        }
	        return result;
		} catch (XQException e) {
			throw new IOException(e);
		}
	}
}

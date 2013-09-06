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

	private int current;
	private int first;
	private int total;
	
	private XQDataSource xqs = null;
	private XQResultSequence rs = null;
	
	private String user = null;
	private String pass = null;
	private String doc = null;
	private String xquery = null;
	
	public VPRecordReader() {
	    LOG.debug("CONSTRUCTOR()");
	}
	
	@Override
	public void close() throws IOException {
	    LOG.debug("close");
	    try {
			rs.close();
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public IntWritable getCurrentKey() throws IOException,
			InterruptedException {
		return new IntWritable(current);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
	    LOG.debug("getCurrentValue()");
	    try {
			return new Text(rs.getItem().getItemAsString(null));
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    LOG.debug("getProgress()");
	    try {
			return (float)rs.getPosition()/(float)rs.count();
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext ctxt)
			throws IOException, InterruptedException {
		
	    LOG.debug("initialize()");
	    
	    setupDbClient(ctxt);
		
		VPInputSplit rangeSplit = (VPInputSplit) split;
		
		first = rangeSplit.getStartPosition();
		current = first;
		total = rangeSplit.getLengh();
		
		readData();
	}

	private void setupDbClient(TaskAttemptContext ctxt) {
		
	    LOG.debug("setupDbClient()");
		
	    Configuration conf = ctxt.getConfiguration();
		String server = conf.get(XmlDBConst.DB_HOST);
		String port = conf.get(XmlDBConst.DB_PORT);
		user = conf.get(XmlDBConst.DB_USER);
		pass = conf.get(XmlDBConst.DB_PASSWORD);
		doc = conf.get(XmlDBConst.DB_DOCUMENT);
		xquery = conf.get(XmlDBConst.DB_XQUERY);
		
		try {
			XQDataSource xqs = new BaseXXQDataSource();
			xqs.setProperty("serverName", server);
			xqs.setProperty("port", port);
		} catch (XQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private void readData() throws IOException {
	    
	    LOG.debug("readData()");
	    
	    try {
			XQConnection conn = xqs.getConnection(user, pass);
			XQPreparedExpression xqpe =
			conn.prepareExpression("doc('"+doc+"')"+xquery+"[position()=("+first+" to "+(first+total-1)+")]");
			rs = xqpe.executeQuery();
		} catch (XQException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
	    LOG.debug("nextKeyValue()");
	    
	    try {
			return rs.next();
		} catch (XQException e) {
			throw new IOException(e);
		}
	}
}

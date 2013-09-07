package uff.dew.vphadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uff.dew.vphadoop.connector.VPInputFormat;



public class Driver {
    
    private static final Log LOG = LogFactory.getLog(Driver.class);
    
	public static void main(String[] args) throws Exception {
		
	    LOG.trace("main()");
	    Configuration conf = new Configuration();
		setDatabaseConfiguration(conf);
		
		Job job = new Job(conf,"vphadoop");
		job.setJarByClass(Driver.class);
		job.setInputFormatClass(VPInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

    private static void setDatabaseConfiguration(Configuration conf) {
        conf.set(XmlDBConst.DB_HOST, "127.0.0.1");
        conf.set(XmlDBConst.DB_PORT, "1984");
        conf.set(XmlDBConst.DB_USER, "admin");
        conf.set(XmlDBConst.DB_PASSWORD, "admin");
        
        conf.set(XmlDBConst.DB_DOCUMENT, "standard");
        conf.set(XmlDBConst.DB_XQUERY, "/site/people/person[?]/name/text()");
    }

}

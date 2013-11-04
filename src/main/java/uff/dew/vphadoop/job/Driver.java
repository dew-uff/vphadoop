package uff.dew.vphadoop.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.connector.VPInputFormat;



public class Driver {
    
    private static final Log LOG = LogFactory.getLog(Driver.class);
    
	public static void main(String[] args) throws Exception {
		
	    LOG.trace("main()");

	    // TODO check arguments
	    
	    Configuration conf = new Configuration();
	    conf.set(VPConst.DB_CONFIGFILE_PATH, args[0]);
        // TODO read this from a file, in a higher level
        conf.set(VPConst.DB_XQUERY, "/site/people/person/name/text()");
		conf.set(VPConst.DB_DOCUMENT, "standard");
        
		Job job = new Job(conf,"vphadoop");
		job.setJarByClass(Driver.class);
		job.setInputFormatClass(VPInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}

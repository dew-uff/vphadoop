package uff.dew.vphadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import uff.dew.vphadoop.connector.VPInputFormat;



public class Driver {
    
	public static class MyMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	    public static final Log LOG = LogFactory.getLog(MyMapper.class
	            .getName());
	    
	    public MyMapper() {

	    }
	    
		@Override
		protected void map(IntWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
		    LOG.debug("Reducing " + key + " and " + value);
			context.write(new IntWritable(value.charAt(0)), value);
		}
	}
	
	   public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

	        public static final Log LOG = LogFactory.getLog(MyReducer.class
	                .getName());
	        
	        public MyReducer() {

	        }

            @Override
            protected void reduce(IntWritable key, Iterable<Text> values,
                    Context context)
                    throws IOException, InterruptedException {
                int count;
                LOG.debug("Processing reduce for: " + key);
                Iterator<Text> it = values.iterator();
                for (count = 0; it.hasNext(); count++);
                LOG.debug("count for: " + key + " = " + count);
                context.write(key, new IntWritable(count));
            }
	        
	        

	    }
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		setDatabaseConfiguration(conf);
		
		Job job = new Job(conf,"vphadoop");
		job.setJarByClass(Driver.class);
		job.setInputFormatClass(VPInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

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

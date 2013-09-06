package uff.dew.vphadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import uff.dew.vphadoop.connector.VPInputFormat;



public class Driver {
	
	public static class MyMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

	    public MyMapper() {
	        //TODO verify
	    }
	    
		@Override
		protected void map(IntWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
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
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true)?0:1);
	}


    private static void setDatabaseConfiguration(Configuration conf) {
        conf.set(XmlDBConst.DB_HOST, "127.0.0.1");
        conf.set(XmlDBConst.DB_PORT, "1984");
        conf.set(XmlDBConst.DB_USER, "admin");
        conf.set(XmlDBConst.DB_PASSWORD, "admin");
        
        conf.set(XmlDBConst.DB_DOCUMENT, "standard");
        conf.set(XmlDBConst.DB_XQUERY, "/site/regions/*/item/name");
    }

}

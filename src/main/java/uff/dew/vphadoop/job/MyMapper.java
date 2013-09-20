package uff.dew.vphadoop.job;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

    private static final Log LOG = LogFactory.getLog(MyMapper.class);
    
    public MyMapper() {

    }

    @Override
    protected void map(IntWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        LOG.trace("Mapping " + key + " and " + value);
        context.write(new IntWritable(value.charAt(0)), value);
    }
}

package uff.dew.vphadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    public static final Log LOG = LogFactory.getLog(MyReducer.class);
    
    public MyReducer() {

    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException {
        int count;
        LOG.trace("Reducing key: " + key);
        Iterator<Text> it = values.iterator();
        for (count = 0; it.hasNext(); count++ ) 
            it.next();
        LOG.trace("count for: " + key + " = " + count);
        context.write(key, new IntWritable(count));
    }
    
    

}
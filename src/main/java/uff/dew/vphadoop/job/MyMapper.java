package uff.dew.vphadoop.job;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<IntWritable, Text, NullWritable, Text> {

    private static final Log LOG = LogFactory.getLog(MyMapper.class);
    
    public static final String PARTIALS_DIR = "partials";
    
    public MyMapper() {

    }

    @Override
    protected void map(IntWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        //LOG.trace("Mapping " + key + " and " + value);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String partialFilename = PARTIALS_DIR + "/partial_" + key.toString() + ".xml";
        //LOG.trace("Saving temp file to HDFS: " + partialFilename);
        OutputStream partialFile = fs.create(new Path(partialFilename));
        partialFile.write(value.getBytes());
        partialFile.close();
        
        context.write(NullWritable.get(), new Text(partialFilename));
    }
}

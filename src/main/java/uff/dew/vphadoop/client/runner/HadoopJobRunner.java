package uff.dew.vphadoop.client.runner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.connector.VPInputFormat;
import uff.dew.vphadoop.job.MyMapper;
import uff.dew.vphadoop.job.MyReducer;

public class HadoopJobRunner {
    
    private static final Log LOG = LogFactory.getLog(HadoopJobRunner.class);
    
    private static final String OUTPUT_PATH = "output";
    
    private String catalogFile;
    private String query;
    private Configuration conf;
    private Path outputPath;
    private Job job;

    public HadoopJobRunner(String query, Configuration conf) {
        this.query = query;
        this.conf = conf;
    }
    
    public void setCatalog(String catalogFile) {
        this.catalogFile = catalogFile;
    }

    private Job setupJob(Configuration conf) throws IOException {
        
        Job job = new Job(conf,"vphadoop");
        job.setJarByClass(this.getClass());
        
        job.setInputFormatClass(VPInputFormat.class);
        
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        outputPath = new Path(OUTPUT_PATH);
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(outputPath)){
            dfs.delete(outputPath, true);
        }
        
        FileOutputFormat.setOutputPath(job, outputPath);
        
        return job;
    }
    
    public void runJob() throws IOException, InterruptedException, ClassNotFoundException {
        
        conf.set(VPConst.XQUERY, query);
        if (catalogFile != null){
            conf.set(VPConst.CATALOG_FILE_PATH, catalogFile);
        }
        
        job = setupJob(conf);
        
        job.waitForCompletion(true);
    }

    public String getResult() {
        try {
            StringBuilder result = new StringBuilder();
            
            FileSystem fs = FileSystem.get(URI.create("vphadoop"), job.getConfiguration());
            //TODO change this
            Path resultFile = new Path("result.xml");
            FSDataInputStream in = fs.open(resultFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String line;
            while ((line = br.readLine())!= null) {
                result.append(line + "\r\n");
            }

            fs.close();
            return result.toString();
        } catch (IOException e) {
            LOG.error("Could not get result file: " + e.getMessage());
            return null;
        }
    }

    public void saveResultInFile(String filename) throws IOException {
        if (filename == null) {
            return;
        }
        
        FileSystem fs = FileSystem.get(URI.create("vphadoop"), job.getConfiguration());
        Path resultFileInHDFS = new Path("result.xml");
        Path localFile = new Path(filename);
        fs.copyToLocalFile(resultFileInHDFS, localFile);
        fs.close();
    }
}

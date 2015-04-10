package uff.dew.vphadoop.client.runner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
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
import uff.dew.vphadoop.client.JobHelper;
import uff.dew.vphadoop.connector.VPInputFormat;
import uff.dew.vphadoop.job.MyMapper;
import uff.dew.vphadoop.job.MyReducer;

public class HadoopJobRunner extends BaseJobRunner {
    
    private static final Log LOG = LogFactory.getLog(HadoopJobRunner.class);
    
    private static final String OUTPUT_PATH = "output";
    
    private String jobConfigFile;
    private String catalogFile;
	
    private Path outputPath;
    
    private Job job;

    public HadoopJobRunner(String query) {
        super(query);
    }
    
    public void setJobConfiguration(String jobConfFilename) {
        this.jobConfigFile = jobConfFilename;        
    }
    
    public void setCatalog(String catalogFile) {
        this.catalogFile = catalogFile;
    }
    
    @Override
    public int getType() {
        return JobRunner.HADOOP;
    }

    @Override
    protected void prepare() throws Exception {
        
    	Configuration conf = new Configuration();
    	conf.addResource(new Path(jobConfigFile));

        conf.set(VPConst.XQUERY, xquery);
        conf.set(VPConst.CATALOG_FILE_PATH, catalogFile);
        
        job = setupJob(conf);
    }
    
    private Job setupJob(Configuration conf) throws IOException {
        
        String localJarsDir = "./dist";
        String hdfsJarsDir = "libs";
        final String dbType = conf.get(VPConst.DB_CONF_TYPE);
        
//        if (dbType == null || dbType.length() == 0) {
//            throw new IOException("dbtype should not be null");
//        }
//        
//        FileFilter fileFilter = new FileFilter() {
//            @Override
//            public boolean accept(File file) {
//                if (dbType.equals(VPConst.DB_TYPE_SEDNA) && file.getName().indexOf("basex") != -1) {
//                    return false;
//                } else if (dbType.equals(VPConst.DB_TYPE_BASEX) && file.getName().indexOf("sedna") != -1){
//                    return false;
//                }
//                return true;
//            }
//        };
//        
//        JobHelper.copyLocalJarsToHdfs(localJarsDir, hdfsJarsDir, fileFilter, conf);
//        JobHelper.addHdfsJarsToDistributedCache(hdfsJarsDir, conf);
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
    
    @Override
    protected void doRunJob() throws IOException, InterruptedException, ClassNotFoundException {
        job.submit();
    }

    @Override
    protected int getMapProgress() {
        try {
            return Math.round(job.mapProgress()*100);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    protected int getReduceProgress() {
        try {
            return Math.round(job.reduceProgress()*100);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public boolean isFinished() {
        try {
            return job.isComplete();
        } catch (IOException e) {
            return true;
        }
    }
    
    public boolean isSuccessFul() {
        try {
            return job.isSuccessful();
        } catch (IOException e) {
            return false;
        }
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

    @Override
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

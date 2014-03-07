package uff.dew.vphadoop.client.runner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import uff.dew.vphadoop.catalog.Catalog;
import uff.dew.vphadoop.client.JobHelper;
import uff.dew.vphadoop.connector.VPInputFormat;
import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;
import uff.dew.vphadoop.job.MyMapper;
import uff.dew.vphadoop.job.MyReducer;

public class HadoopJobRunner extends BaseJobRunner {
    
    private static final Log LOG = LogFactory.getLog(HadoopJobRunner.class);
    
    private static final String DB_CONFIG_PATH = "configuration.xml";
    private static final String OUTPUT_PATH = "output";
    
    private String jobTrackerHost;
    private int jobTrackerPort;
    private String namenodeHost;
    private int namenodePort;
	private int numFragments;
	
    private String dbType;
    
    private String dbConfFile;
    
    private Path outputPath;
    
    private Job job;

    public HadoopJobRunner(String query) {
        super(query);
    }
    
    public void setHadoopConfiguration(String jobTrackerHost, 
            int jobTrackerPort, String namenodeHost, int namenodePort, int numFragments) {
        this.jobTrackerHost = jobTrackerHost;
        this.jobTrackerPort = jobTrackerPort;
        this.namenodeHost = namenodeHost;
        this.namenodePort = namenodePort; 
        this.numFragments = numFragments;
    }
    
    public void setDbConfiguration(String type, String host, int port, String user, 
            String password, String dbName) throws IOException {
        
        String configPath = writeDbConfigurationFile(type, host, port, user, password, dbName);
        setDbConfiguration(configPath);
    }
    
    public void setDbConfiguration(String dbConfFile) {
        this.dbConfFile = dbConfFile;
        try {
            FileInputStream fis = new FileInputStream(dbConfFile);
            DatabaseFactory.produceSingletonDatabaseObject(fis);
            Database dbObject = DatabaseFactory.getSingletonDatabaseObject();
            this.dbType = dbObject.getType();
            fis.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void setCatalog(String catalogFile) {

    	Catalog.get().setDatabaseObject(DatabaseFactory.getSingletonDatabaseObject());
    	
    	if (catalogFile != null) {
    		try {
            	FileInputStream fis = new FileInputStream(catalogFile);
            	Catalog.get().populateCatalogFromFile(fis);
    		} 
    		catch (FileNotFoundException e) {
    			Catalog.get().createCatalog();
    			Catalog.get().saveCatalogToFile(catalogFile);
    		}
    	}
    	else {
    		Catalog.get().setDbMode(true);
    	}
    }
    
    @Override
    public int getType() {
        return JobRunner.HADOOP;
    }

    @Override
    protected void prepare() throws IOException {
        
    	Configuration conf = new Configuration();

        conf.set("fs.default.name","hdfs://"+jobTrackerHost+":"+jobTrackerPort+"/");
        conf.set("mapred.job.tracker", namenodeHost + ":" + namenodePort);
        conf.setInt("mapred.task.timeout",0);
        conf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
        conf.setInt(VPConst.SVP_NUM_FRAGMENTS, numFragments);
        
        //TODO read this from interface
        conf.set(VPConst.DB_XQUERY, xquery);
        conf.set(VPConst.DB_CONFIGFILE_PATH, DB_CONFIG_PATH);
        
        copyDbConfigurationToHDFS(conf);
        
        job = setupJob(conf);
    }
    
    private void copyDbConfigurationToHDFS(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (dbConfFile == null) {
        	throw new IOException("Config file was not given nor created!");
        }
        fs.copyFromLocalFile(new Path(dbConfFile), new Path(DB_CONFIG_PATH));
        fs.close();
    }

    private String writeDbConfigurationFile(String dbType, String dbHost, int dbPort, String dbUser, String dbPassword, String dbName) throws IOException {
        
    	File file = File.createTempFile("configuration", "xml");
    	
    	FileOutputStream fos = new FileOutputStream(file);
    	
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        bw.write("<?xml version=\"1.0\"?>\n");
        bw.write("<vphadoop>\n");
        bw.write("<database>\n");
        bw.write("<"+DatabaseFactory.CONFIG_FILE_TYPE_ELEMENT+">"+ dbType +"</"+DatabaseFactory.CONFIG_FILE_TYPE_ELEMENT+">\n");
        bw.write("<"+DatabaseFactory.CONFIG_FILE_HOST_ELEMENT+">"+ dbHost+"</"+DatabaseFactory.CONFIG_FILE_HOST_ELEMENT+">\n");
        bw.write("<"+DatabaseFactory.CONFIG_FILE_PORT_ELEMENT+">"+ dbPort+"</"+DatabaseFactory.CONFIG_FILE_PORT_ELEMENT+">\n");
        bw.write("<"+DatabaseFactory.CONFIG_FILE_USERNAME_ELEMENT+">"+ dbUser+"</"+DatabaseFactory.CONFIG_FILE_USERNAME_ELEMENT+">\n");
        bw.write("<"+DatabaseFactory.CONFIG_FILE_PASSWORD_ELEMENT+">"+ dbPassword+"</"+DatabaseFactory.CONFIG_FILE_PASSWORD_ELEMENT+">\n");
        bw.write("<"+DatabaseFactory.CONFIG_FILE_DATABASE_ELEMENT+">"+ dbName +"</"+DatabaseFactory.CONFIG_FILE_DATABASE_ELEMENT+">\n");
        bw.write("</database>\n");
        bw.write("</vphadoop>\n");
        bw.close();
        fos.close();
        
        return file.getPath();
    }
    
    private Job setupJob(Configuration conf) throws IOException {
        
        String localJarsDir = "./dist";
        String hdfsJarsDir = "/user/hduser/libs";
        FileFilter fileFilter = new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (dbType.equals(DatabaseFactory.TYPE_SEDNA) && file.getName().indexOf("basex") != -1) {
                    return false;
                } else if (dbType.equals(DatabaseFactory.TYPE_BASEX) && file.getName().indexOf("sedna") != -1){
                    return false;
                }
                return true;
            }
        };
        
        JobHelper.copyLocalJarsToHdfs(localJarsDir, hdfsJarsDir, fileFilter, conf);
        JobHelper.addHdfsJarsToDistributedCache(hdfsJarsDir, conf);
        Job job = new Job(conf,"vphadoop");
        
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

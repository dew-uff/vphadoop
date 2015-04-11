package uff.dew.vphadoop.job;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import uff.dew.svp.ExecutionContext;
import uff.dew.svp.FinalResultComposer;
import uff.dew.svp.db.DatabaseException;
import uff.dew.vphadoop.VPConst;

public class MyReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

    private static final Log LOG = LogFactory.getLog(MyReducer.class);
    
    public MyReducer() {

    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException {

        long startTimestamp = System.currentTimeMillis();
        
		// construct db object from configuration file 
        Configuration conf = context.getConfiguration();

        FinalResultComposer composer = new FinalResultComposer();
        
        // the constructFinalQuery (below) was originally executed using the same context
        // previously used to retrieve a partial result (at the coordinator node). 
        // That means that some singletons objects were populated when compiling the final 
        // result. Now we don't have that information, so we need to restore it.
        FileSystem fs = FileSystem.get(conf);
        InputStream file = fs.open(new Path("hack.txt"));
        composer.setExecutionContext(ExecutionContext.restoreFromStream(file));
        file.close();
        
        try {
            composer.setDatabaseInfo(conf.get(VPConst.DB_CONF_HOST), 
                conf.getInt(VPConst.DB_CONF_PORT,5050), conf.get(VPConst.DB_CONF_USERNAME), 
                conf.get(VPConst.DB_CONF_PASSWORD), conf.get(VPConst.DB_CONF_DATABASE), 
                conf.get(VPConst.DB_CONF_TYPE));
        }
        catch (DatabaseException e) {
            LOG.error("Something wrong with database configuration.",e);
        }
        
        
        // put every partial result in a temp collection at the database
        loadPartialsIntoDatabase(composer, values, context);
        
        long loadingTimestamp = System.currentTimeMillis();
        long dbLoadingTime = (loadingTimestamp - startTimestamp);
        LOG.debug("VP:reducer:tempDBLoadingTime: " + dbLoadingTime + " ms.");
        context.getCounter(VPCounters.COMPOSING_TIME_TEMP_COLLECTION_CREATION).increment(dbLoadingTime);
        
        Path path = new Path("result.xml");
        OutputStream resultWriter = fs.create(path);
        composer.combinePartialResults(resultWriter,false);

        long reduceQueryTimestamp = System.currentTimeMillis();
        
        long queryExecutionTime = (reduceQueryTimestamp - loadingTimestamp);
        LOG.debug("VP:reducer:query execution total time: " + queryExecutionTime + " ms.");
        context.getCounter(VPCounters.COMPOSING_TIME_TEMP_COLLECTION_QUERY_EXEC).increment(queryExecutionTime);
        context.getCounter(VPCounters.COMPOSING_TIME).increment(dbLoadingTime + queryExecutionTime);
        
        resultWriter.flush();
        resultWriter.close();
    }
    

    private void loadPartialsIntoDatabase(FinalResultComposer composer, Iterable<Text> values, Context context) throws IOException {
    	long timestamp = System.currentTimeMillis();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        boolean zip = context.getConfiguration().getBoolean(VPConst.COMPRESS_DATA, true);

        for(Text filename : values) {
            Path src = new Path(filename.toString());
            if (zip) {
                LOG.debug("Extracting partial " + filename.toString() + ".");
                
                BufferedInputStream is = new BufferedInputStream(fs.open(src));
                ZipInputStream zis = new ZipInputStream(is);
                zis.getNextEntry();
                composer.loadPartial(zis);
                zis.close();
            }
            else {
                composer.loadPartial(fs.open(src));
            }
        }
        LOG.debug("loadPartialsIntoDb:time to copy from hdfs to local: " + (System.currentTimeMillis() - timestamp) + " ms.");
        timestamp = System.currentTimeMillis();
    }
}
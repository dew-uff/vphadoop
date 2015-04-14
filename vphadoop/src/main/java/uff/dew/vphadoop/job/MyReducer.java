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

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("result.xml");
        OutputStream resultWriter = fs.create(path);
        FinalResultComposer composer = new FinalResultComposer(resultWriter);

        try {
            composer.setDatabaseInfo(conf.get(VPConst.DB_CONF_HOST), 
                conf.getInt(VPConst.DB_CONF_PORT,5050), conf.get(VPConst.DB_CONF_USERNAME), 
                conf.get(VPConst.DB_CONF_PASSWORD), conf.get(VPConst.DB_CONF_DATABASE), 
                conf.get(VPConst.DB_CONF_TYPE));
        }
        catch (DatabaseException e) {
            throw new IOException("Something wrong with database configuration.",e);
        } 
        
        // the constructFinalQuery (below) was originally executed using the same context
        // previously used to retrieve a partial result (at the coordinator node). 
        // That means that some singletons objects were populated when compiling the final 
        // result. Now we don't have that information, so we need to restore it.
        InputStream file = fs.open(new Path("hack.txt"));
        composer.setExecutionContext(ExecutionContext.restoreFromStream(file));
        file.close();
        
        // put every partial result in a temp collection at the database
        loadPartialsIntoDatabase(composer, values, context);
        
        long loadingTimestamp = System.currentTimeMillis();
        long dbLoadingTime = (loadingTimestamp - startTimestamp);
        LOG.debug("Time to load partials: " + dbLoadingTime + " ms.");
        context.getCounter(VPCounters.COMPOSING_TIME_LOAD_PARTIALS).increment(dbLoadingTime);
        
        composer.combinePartialResults();

        long compositionTimestamp = System.currentTimeMillis();
        long compositionExecutionTime = (compositionTimestamp - loadingTimestamp);
        LOG.debug("Time to combine all partials: " + compositionExecutionTime + " ms.");
        context.getCounter(VPCounters.COMPOSING_TIME_COMBINE_PARTIALS).increment(compositionExecutionTime);
        context.getCounter(VPCounters.COMPOSING_TIME_TOTAL).increment(dbLoadingTime + compositionExecutionTime);
        
        resultWriter.flush();
        resultWriter.close();
    }
    

    private void loadPartialsIntoDatabase(FinalResultComposer composer, Iterable<Text> values, Context context) throws IOException {

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
    }
}
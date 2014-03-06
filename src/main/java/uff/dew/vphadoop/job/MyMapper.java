package uff.dew.vphadoop.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.fragmentacaoVirtualSimples.SubQuery;

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
    
    private String subquery = new String();
    
    public MyMapper() {

    }

    @Override
    protected void map(IntWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        
        long start = System.currentTimeMillis();
        
        // value is the fragment file from SVP
        String fragment = value.toString(); 
        
        // need to process it to extract the subquery
        processFragment(fragment);
        
        // execute query, saving result to a partial file in hdfs
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String partialFilename = PARTIALS_DIR + "/partial_" + key.toString() + ".xml";
        Path partialPath = new Path(partialFilename);
        OutputStream partialFile = fs.create(partialPath);
        boolean hasResults = SubQuery.executeSubQuery(subquery, partialFile);
        partialFile.close();
        
        // if it doesn't have results, delete the partial file
        if (!hasResults) {
            fs.delete(partialPath, false);
        }

        long timeProcessing = System.currentTimeMillis() - start;
        incrementNodeProcessingTime(timeProcessing, context);
        LOG.debug("VP:mapper:executionTime: " + timeProcessing + " ms.");
        
        if (hasResults) {
        	context.getCounter(VPCounters.PARTITIONS_WITH_RESULT).increment(1);
            // reducer will receive a list of filenames containing the fragments
            context.write(NullWritable.get(), new Text(partialFilename));
        }
    }

    private void incrementNodeProcessingTime(long timeProcessing, Context context) {
    	// get name of the node
		String hostname;
		try {
			hostname = InetAddress.getLocalHost().getHostName();
			// TODO
			if (hostname.equals("XPS14")) {
	    		context.getCounter(VPCounters.NODE0_TOTAL_TIME).increment(timeProcessing);
	    	}			
			
		} catch (UnknownHostException e) {
			LOG.warn("Could not determint node name. Won't log time per node!");
		}
	}

	private void processFragment(String fragment) throws IOException {

        Query q = Query.getUniqueInstance(true);
        SubQuery sbq = SubQuery.getUniqueInstance(true);
        
        StringReader sr = new StringReader(fragment);
        BufferedReader buff = new BufferedReader(sr);

        String line;
        while((line = buff.readLine()) != null){    

            if (!line.toUpperCase().contains("<ORDERBY>") && !line.toUpperCase().contains("<ORDERBYTYPE>") 
                    && !line.toUpperCase().contains("<AGRFUNC>")) {                         
                subquery = subquery + " " + line;
            }
            else {
                // obter as cláusulas do orderby e de funçoes de agregaçao
                if (line.toUpperCase().contains("<ORDERBY>")){
                    String orderByClause = line.substring(line.indexOf("<ORDERBY>")+"<ORDERBY>".length(), line.indexOf("</ORDERBY>"));
                    q.setOrderBy(orderByClause);
                }
                
                if (line.toUpperCase().contains("<ORDERBYTYPE>")){
                    String orderByType= line.substring(line.indexOf("<ORDERBYTYPE>")+"<ORDERBYTYPE>".length(), line.indexOf("</ORDERBYTYPE>"));                         
                    q.setOrderByType(orderByType);
                }
                
                if (line.toUpperCase().contains("<AGRFUNC>")){ // soma 1 para excluir a tralha contida apos a tag <AGRFUNC>
                    
                    String aggregateFunctions = line.substring(line.indexOf("<AGRFUNC>")+"<AGRFUNC>".length(), line.indexOf("</AGRFUNC>"));
                                                
                    if (!aggregateFunctions.equals("") && !aggregateFunctions.equals("{}")) {
                        String[] functions = aggregateFunctions.split(","); // separa todas as funções de agregação utilizadas no return statement.
                    
                        if (functions!=null) {
                            
                            for (String keyMap: functions) {
                    
                                String[] hashParts = keyMap.split("=");
                                
                                if (hashParts!=null) {
                    
                                    q.setAggregateFunc(hashParts[0], hashParts[1]); // o par CHAVE, VALOR
                                }
                            }
                        }
                    }                       
                }                       
            }
        }
        
        subquery = subquery.trim();
        sbq.setConstructorElement(SubQuery.getConstructorElement(subquery));
    }
}

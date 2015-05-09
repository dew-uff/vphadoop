package uff.dew.vphadoop.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uff.dew.vphadoop.client.runner.HadoopJobRunner;

/**
 * The main class for the CLI version of VPHadoop.
 * 
 * @author Gabriel Tessarolli
 *
 */
public class VPCLI extends Configured implements Tool {
	
    private static final Log LOG = LogFactory.getLog(VPCLI.class);
    
    private static String resultFile;
	private static String catalogFile;
	
    /**
     * The main function for CLI version
     * 
     * @param args Array of parameters.
     */
    public static void main(String[] args) {
        
        int res;
        try {
            res = ToolRunner.run(new Configuration(), new VPCLI(), args);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            res = -1;
        }
        System.exit(res);
    }
     
    @Override
    public int run(String[] args) throws Exception {
    
        if (args.length < 2) {
        	LOG.error("Usage: vphadoop "
                            + "<query.xq> "            //0
                            + "<output_file> "         //1
                            + "[<catalog.xml>]");      //2
            return 1;
        }

        // query file
        String query = null;
        try {
            query = readContentFromFile(args[0]);
            verifyQuery(query);
        } catch (FileNotFoundException e) {
        	LOG.error("Query file was not found!");
            return 1;
        } catch (IOException e) {
            LOG.error("Something went wrong while reading query file!");
            return 1;
        } 
        
        resultFile = args[1];

        // has catalog
        if (args.length == 3) {
        	catalogFile = args[2];
        }
        else {
        	catalogFile = null;
        }
        
        // process the query
        try {
            processQuery(query);
            
        } catch (IOException e) {
        	LOG.error("Something went wrong while processing the query: "
                            + e.getMessage());
            return 1;
        }
        
        return 0;
    }

    /**
     * Load the file content into a String object
     * 
     * @param filename The file
     * @return the content of the file in a string object
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static String readContentFromFile(String filename) throws FileNotFoundException, IOException {
        
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String everything = null;
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append('\n');
                line = br.readLine();
            }
            everything = sb.toString().trim();
        } finally {
            br.close();
        }
        
        return everything;
    }
    
    /**
     * Checks whether the query fulfills the requirements to process it. If
     * any problem is found, an exception is raised.<br/>
     *   1. must start and end with a constructor element<br/> 
     *   2. must not use the text() function (not supported)<br/>
     *   3. must have a XML element after return clause<br/>
     * 
     * @param query The query to be checked
     * @throws Exception
     */
    private static void verifyQuery(String query) throws Exception {
        String returnClause = query.substring(query.indexOf("return")+6, query.length());
        
        if (query.indexOf("<") > 0 || query.lastIndexOf(">") != query.length()-1) {
            throw new Exception("A consulta de entrada deve iniciar e "
                    + "terminar com um elemento construtor. Exemplo: <resultado> "
                    + "{ for $var in ... } </resultado>.");
        }
        else if (query.toUpperCase().indexOf("/TEXT()") != -1) {
            throw new Exception("O parser deste programa não aceita a função text(). "
                    + "Especifique somente os caminhos xpath para acessar os "
                    + "elementos nos documentos XML.");
        }
        else if (returnClause.trim().charAt(0) != '<') {
            
            throw new Exception("É obrigatória a especificação de um elemento XML após "
                    + "a cláusula return. Ex.: <results> { for $var ... return "
                    + "<elemName> ... </elemName> } </results>");
        }
    }
    
    /**
     * Process the query using Hadoop
     * 
     * @param query The query to be processed
     * @param dbConf The configuration file for DB access
     * @throws IOException
     * @throws JobException
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    private void processQuery(String query) throws IOException, ClassNotFoundException, InterruptedException {
        
        Configuration conf = getConf();
        HadoopJobRunner hadoopJob = new HadoopJobRunner(query, conf);
        hadoopJob.setCatalog(catalogFile);
        
        long startTimestamp = System.currentTimeMillis();
        
        hadoopJob.runJob();
        
        long hadoopExecutionTimestamp = System.currentTimeMillis();
        
        LOG.info("Hadoop execution time: " + (hadoopExecutionTimestamp - startTimestamp) + " ms.");
        
        hadoopJob.saveResultInFile(resultFile);
        
        long copyTimestamp = System.currentTimeMillis();
        
        LOG.info("Copy result time: " + (copyTimestamp - hadoopExecutionTimestamp) + " ms.");
    }
}

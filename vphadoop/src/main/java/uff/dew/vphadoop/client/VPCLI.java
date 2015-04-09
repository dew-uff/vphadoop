package uff.dew.vphadoop.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.vphadoop.client.runner.HadoopJobRunner;
import uff.dew.vphadoop.client.runner.JobListener;
import uff.dew.vphadoop.client.runner.JobRunner;

/**
 * The main class for the CLI version of VPHadoop.
 * 
 * @author Gabriel Tessarolli
 *
 */
public class VPCLI {
	
    private static final Log LOG = LogFactory.getLog(VPCLI.class);
    
    private static String resultFile;
	private static String catalogFile;
	
    private static JobRunner job;
    
    /**
     * Listener to get updates from the Job. When map or reduce status
     * changes, it is notified, so the user can be notified too
     */
    private static JobListener myJobListener = new JobListener() {
        
        int mapProgress, reduceProgress;
        
        @Override
        public void reduceProgressChanged(int value) {
            reduceProgress = value;
            printStatus();
            
        }
        
        @Override
        public void mapProgressChanged(int value) {
            mapProgress = value;
            printStatus();
        }
        
        @Override
        public void completed(boolean successful) {
            if (successful) {
            	LOG.info("Total processing time: " + (System.currentTimeMillis()-startTimestamp) + " ms.");
                try {
                    saveResult();
                } catch (IOException e) {
                    LOG.error("Something went wrong while saving the result: "
                            + e.getMessage());
                    System.exit(1);
                }                
            }
            else {
                LOG.error("Erro!!");
                System.exit(1);
            }
        }
        
        private void printStatus() {
            LOG.info("Map: " + mapProgress + "%   Reduce: " + reduceProgress + "%");
        }
        
    };
	private static long startTimestamp;

    /**
     * The main function for CLI version
     * 
     * @param args Array of parameters.
     */
    public static void main(String[] args) {

        if (args.length < 3) {
        	LOG.error("Usage: java -jar vphadoop.jar "
        	                + "<jobconfiguration.xml> " //0
                            + "<query.xq> "            //1
                            + "<output_file> "         //2
                            + "[<catalog.xml>]");      //3
            System.exit(0);
        }

        // query file
        String query = null;
        try {
            query = readContentFromFile(args[1]);
            verifyQuery(query);
        } catch (FileNotFoundException e) {
        	LOG.error("Query file was not found!");
            System.exit(1);
        } catch (IOException e) {
            LOG.error("Something went wrong while reading query file!");
            System.exit(1);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            System.exit(1);
        }
        
        resultFile = args[2];

        // has catalog
        if (args.length == 4) {
        	catalogFile = args[3];
        }
        else {
        	catalogFile = null;
        }
        
        // process the query
        try {
            processQuery(query,args[0]);
        } catch (IOException e) {
        	LOG.error("Something went wrong while processing the query: "
                            + e.getMessage());
            System.exit(1);
        } catch (JobException e) {
        	LOG.error("Something went wrong while processing the query: "
                            + e.getMessage());
            System.exit(1);
        }
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
     */
    private static void processQuery(String query, String jobConfFilename) throws IOException, JobException {
        HadoopJobRunner hadoopJob = new HadoopJobRunner(query);
        hadoopJob.setJobConfiguration(jobConfFilename);
        hadoopJob.setCatalog(catalogFile);
        hadoopJob.addListener(myJobListener);
        
        startTimestamp = System.currentTimeMillis();
        
        hadoopJob.runJob();
        
        job = hadoopJob;
    }

    /**
     * Hook function invoked to get the result when the job is successfully completed
     * 
     * @throws IOException
     */
    protected static void saveResult() throws IOException {
        job.saveResultInFile(resultFile);
    }
    
}

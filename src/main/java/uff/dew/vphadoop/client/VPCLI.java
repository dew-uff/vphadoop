package uff.dew.vphadoop.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

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
    
    private static String resultFile;
    private static String jobtrackerHost;
    private static String namenodeHost;
	private static int jobtrackerPort;
	private static int namenodePort;
	
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
                try {
                    saveResult();
                } catch (IOException e) {
                    System.out.println("Something went wrong while saving the result: "
                            + e.getMessage());
                    System.exit(1);
                }                
            }
            else {
                System.out.println("Erro!!");
            }
        }
        
        private void printStatus() {
            System.out.println("Map: " + mapProgress + "%   Reduce: " + reduceProgress + "%");
        }
        
    };

    /**
     * The main function for CLI version
     * 
     * @param args Array of parameters.
     */
    public static void main(String[] args) {

        if (args.length < 7) {
            System.out
                    .println("Usage: java -jar vphadoop.jar "
                    		+ "<dbconfiguration.xml> " //0
                            + "<query.xq> "            //1
                            + "<output_file> "         //2
                            + "<jobtrackerhost> "      //3
                            + "<jobtrackerport> "      //4
                            + "<namenodehost> "        //5
                            + "<namenodeport> "        //6
                            + "<catalog.xml>");        //7
            System.exit(0);
        }

        // configuration file
        File f = new File(args[0]);
        if (!f.exists()) {
            System.err.println(args[0] + " DB configuration file not found!");
            System.exit(1);
        }

        // query file
        String query = null;
        try {
            query = readContentFromFile(args[1]);
            verifyQuery(query);
        } catch (FileNotFoundException e) {
            System.out.println("Query file was not found!");
            System.exit(1);
        } catch (IOException e) {
            System.out
                    .println("Something went wrong while reading query file!");
            System.exit(1);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        
        try {
            jobtrackerPort = Integer.parseInt(args[4]);        	
            namenodePort = Integer.parseInt(args[6]);
        } catch (NumberFormatException e) {
        	System.out.println("Ports must be numbers! (" + e.getMessage() + ")");
        	System.exit(1);
        }
        
        resultFile = args[2];
        jobtrackerHost = args[3];
        namenodeHost = args[5];


        // process the query
        try {
            processQuery(query,args[0]);
        } catch (IOException e) {
            System.out
                    .println("Something went wrong while processing the query: "
                            + e.getMessage());
            System.exit(1);
        } catch (JobException e) {
            System.out
                    .println("Something went wrong while processing the query: "
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
    private static void processQuery(String query, String dbConf) throws IOException, JobException {
        HadoopJobRunner hadoopJob = new HadoopJobRunner(query);
        hadoopJob.setHadoopConfiguration(jobtrackerHost, jobtrackerPort, namenodeHost, namenodePort);
        hadoopJob.setDbConfiguration(dbConf);
        hadoopJob.addListener(myJobListener);
        
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

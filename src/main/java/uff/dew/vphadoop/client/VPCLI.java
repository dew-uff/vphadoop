package uff.dew.vphadoop.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import uff.dew.vphadoop.client.runner.HadoopJobRunner;
import uff.dew.vphadoop.client.runner.JobListener;
import uff.dew.vphadoop.client.runner.JobRunner;

public class VPCLI {
    
    private static String resultFile;
    private static String jobtracker;
    private static String namenode;
    
    private static JobRunner job;
    
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

    public static void main(String[] args) {

        if (args.length < 3) {
            System.out
                    .println("Usage: java -jar vphadoop.jar <dbconfiguration.xml> <query.xq> <output_file> <jobtracker> <namenode>");
            System.exit(0);
        }

        File f = new File(args[0]);
        if (!f.exists()) {
            System.err.println(args[0] + " DB configuration file not found!");
            System.exit(1);
        }

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
        
        resultFile = args[2];
        jobtracker = args[3];
        namenode = args[4];

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
    
    private static void verifyQuery(String query) throws Exception {
        String returnClause = query.substring(query.indexOf("return")+6, query.length());
        
        if (query.indexOf("<") > 0 || query.lastIndexOf(">") != query.length()-1) {
            throw new Exception("A consulta de entrada deve iniciar e terminar com um elemento construtor. Exemplo: <resultado> { for $var in ... } </resultado>.");
        }
        else if (query.toUpperCase().indexOf("/TEXT()") != -1) {
            throw new Exception("O parser deste programa não aceita a função text(). Especifique somente os caminhos xpath para acessar os elementos nos documentos XML.");
        }
        else if (returnClause.trim().charAt(0) != '<') {
            
            throw new Exception("É obrigatória a especificação de um elemento XML após a cláusula return. Ex.: <results> { for $var ... return <elemName> ... </elemName> } </results>");
        }
    }
    
    private static void processQuery(String query, String dbConf) throws IOException, JobException {
        HadoopJobRunner hadoopJob = new HadoopJobRunner(query);
        hadoopJob.setHadoopConfiguration(jobtracker, 9000, namenode, 9001);
        hadoopJob.setDbConfiguration(dbConf);
        hadoopJob.addListener(myJobListener);
        
        hadoopJob.runJob();
        
        job = hadoopJob;
    }

    
    protected static void saveResult() throws IOException {
        job.saveResultInFile(resultFile);
        
    }
    
}

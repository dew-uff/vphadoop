package uff.dew.vphadoop.connector;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import mediadorxml.engine.XQueryEngine;
import mediadorxml.fragmentacaoVirtualSimples.ExistsJoinOperation;
import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.fragmentacaoVirtualSimples.SimpleVirtualPartitioning;
import mediadorxml.fragmentacaoVirtualSimples.SubQuery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.vphadoop.VPConst;

public class VPInputFormat extends InputFormat<IntWritable, Text> {
    
    private static final Log LOG = LogFactory.getLog(VPInputFormat.class);
    private ArrayList<String> docQueries;
    private ArrayList<String> docQueriesWithoutFragmentation;
    private String originalQuery;
    private String inputQuery;
    private int nsplits = 10;
    private int nrecords = 5;
    private int nfragments = 50;
    private String xquery;
    private XQueryEngine engine;

    @Override
    public RecordReader<IntWritable, Text> createRecordReader(InputSplit in,
            TaskAttemptContext ctxt) throws IOException, InterruptedException {

        return new VPRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext ctxt) throws IOException,
            InterruptedException {

        Configuration conf = ctxt.getConfiguration();
        
        // the query to process
        inputQuery = conf.get(VPConst.XQUERY);
        nsplits = conf.getInt(VPConst.SVP_NUM_SPLITS, 10);
        nrecords = conf.getInt(VPConst.SVP_RECORDS_PER_SPLIT, 5);
        
        nfragments = nsplits * nrecords;
        
        long start = System.currentTimeMillis();
        
        try {
        	// to mimic PartiX-VP flow.
            svpPressed();
        } 
        catch (Exception e) {
            throw new IOException(e);
        }
        
        long partitionTime = System.currentTimeMillis() - start;
        
        LOG.info("VP:partitioningTime: " + partitionTime + "ms");
        
        List<InputSplit> splits = new ArrayList<InputSplit>();

        List<String> queries = new ArrayList<String>(getQueries());
        int qcount = 0;
        
        for (int i = 0; i < nsplits; i++) {
            List<String> qs = new ArrayList<>(nrecords);
            for (int j = 0; j < nrecords; j++) {
                qs.add(queries.get(qcount));
                LOG.trace("Split["+i+"]Record["+j+"] = Queries["+qcount+"] = " + queries.get(qcount));
                qcount++;
            }
            int initialPos = Integer.parseInt(SubQuery.getIntervalBeginning(queries.get(0)));
            InputSplit is = new VPInputSplit(initialPos, qs);
            splits.add(is);
        }
        
        //TODO HACK
        FileSystem fs = FileSystem.get(conf);
        Path hack2 = new Path("hack2.txt");
        if (fs.exists(hack2)) {
        	fs.delete(hack2, false);
        }
        OutputStream hack = fs.create(new Path("hack.txt"));
        hack.write(queries.get(0).getBytes());
        hack.close();
        
        LOG.debug("# of splits: " + splits.size());
        
        return splits;
    }

    private void xqueryPressed() throws Exception {
        
        Query q = Query.getUniqueInstance(true);
        
        /* Define o tipo de consulta (collection() ou doc()) e, caso seja sobre uma coleção 
         * retorna as sub-consultas geradas, armazenando-as no objeto docQueries.
         */
        q.setInputQuery(inputQuery);
        docQueries = q.setqueryExprType(inputQuery);
        
        if ( docQueries!=null && docQueries.size() > 0 ){ // é diferente de null, quando consulta de entrada for sobre uma coleção
            
            docQueriesWithoutFragmentation = docQueries;                                
        }
        else if (q.getqueryExprType()!=null && q.getqueryExprType().equals("document")) { // consulta de entrada sobre um documento. 
            q.setInputQuery(inputQuery);
        }
        else if (q.getqueryExprType()!=null && q.getqueryExprType().equals("collection")) { // consulta de entrada sobre uma coleção.
           throw new IOException("Erro ao gerar sub-consultas para a coleção indicada. Verifique a consulta de entrada.");
        }
    }

    private void svpPressed() throws Exception {
        
        xqueryPressed();
        
        SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(false);
        Query q = Query.getUniqueInstance(true);
        if (q.getqueryExprType()!=null && q.getqueryExprType().equals("collection")){
            
            if (docQueries!=null){
                
                originalQuery = inputQuery;
                for (String docQry : docQueries) {
                    
                    inputQuery = docQry;
                    
                    executeXQuery();                                            
                }
                
               inputQuery = originalQuery;
            }
        }
        else {
            svp.setNumberOfNodes(nfragments);
            svp.setNewDocQuery(true);                                                       
            executeXQuery();                            
        }
    }
    

    private void executeXQuery() throws IOException {
        
        ExistsJoinOperation ej = new ExistsJoinOperation(inputQuery);
        ej.verifyInputQuery();
        Query q = Query.getUniqueInstance(true);
        q.setLastReadCardinality(-1);
        q.setJoinCheckingFinished(false);               
        
        
        if ((xquery == null) || (!xquery.equals(inputQuery))){ 
            xquery = inputQuery; //  consulta de entrada                  
        }       
        
        
        if ( q.getqueryExprType()!= null && !q.getqueryExprType().contains("collection") ) { // se a consulta de entrada não contém collection, execute a fragmentação virtual.
        
            engine = new XQueryEngine();
            engine.execute(xquery, false); // Para debugar o parser, passe o segundo parâmetro como true.                
            
            q.setJoinCheckingFinished(true);
            
            if (q.isExistsJoin()){
                q.setOrderBy("");                       
                engine.execute(xquery, false); // Executa pela segunda vez, porém desta vez fragmenta apenas um dos joins
            }               
            
        }
        else {  // se contem collection         
                            
            // Efetua o parser da consulta para identificar os elementos contidos em funções de agregação ou order by, caso existam.
            q.setOrderBy("");
            engine = new XQueryEngine();
            engine.execute(originalQuery, false);
            
            if (q.getPartitioningPath()!=null && !q.getPartitioningPath().equals("")) {
                SimpleVirtualPartitioning svp = new SimpleVirtualPartitioning();
                svp.setCardinalityOfElement(q.getLastCollectionCardinality());
                svp.setNumberOfNodes(nfragments);                       
                svp.getSelectionPredicateToCollection(q.getVirtualPartitioningVariable(), q.getPartitioningPath(), xquery);                                         
                q.setAddedPredicate(true);
            }
        }
    }
    
    private List<String> getQueries() throws IOException {
        
        Query q = Query.getUniqueInstance(true);
        SubQuery sbq = SubQuery.getUniqueInstance(true);       
        
        SimpleVirtualPartitioning svp = SimpleVirtualPartitioning.getUniqueInstance(true);
        LOG.info("Cardinality of selected element: " + svp.getCardinalityOfElement());
        
        if ( sbq.getSubQueries()!=null && sbq.getSubQueries().size() > 0 ){
            
            List<String> results = new ArrayList<String>(sbq.getSubQueries().size());
            
            for ( String initialFragments : sbq.getSubQueries() ) {
                StringBuilder result = new StringBuilder();

                result.append("<ORDERBY>" + q.getOrderBy() + "</ORDERBY>\r\n");
                result.append("<ORDERBYTYPE>" + q.getOrderByType() + "</ORDERBYTYPE>\r\n");
                result.append("<AGRFUNC>" + (q.getAggregateFunctions()!=null?q.getAggregateFunctions():"") + "</AGRFUNC>#\r\n");
                
                result.append(initialFragments);
                results.add(result.toString());
            
            }
            
            return results;
        }
        else {
            
            if ( this.docQueriesWithoutFragmentation != null && this.docQueriesWithoutFragmentation.size() >0 ) { // para consulta que nao foram fragmentadas pois nao ha relacionamento de 1 para n.
  
                return this.docQueriesWithoutFragmentation;
            }
            else { // nao gerou fragmentos e nao ha consultas de entrada. Ocorreu algum erro durante o parser da consulta. 
                return null;
            }
        }
    }
}

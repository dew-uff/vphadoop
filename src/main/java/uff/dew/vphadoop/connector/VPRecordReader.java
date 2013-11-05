package uff.dew.vphadoop.connector;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.fragmentacaoVirtualSimples.SubQuery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import uff.dew.vphadoop.VPConst;
import uff.dew.vphadoop.db.Catalog;



public class VPRecordReader extends RecordReader<IntWritable, Text> {
    
    public static final Log LOG = LogFactory.getLog(RecordReader.class);
    
    // flags to indicate current position
    private static final int NOT_STARTED = -1;
    private static final int DONE = -2;
	
	private int current = NOT_STARTED;
	private int startPos = 0;
	
	private String xquery = null;
	private String result = null;
	
	public VPRecordReader() {
	    LOG.trace("CONSTRUCTOR() " + this);
	}
	
	@Override
	public void close() throws IOException {
	}

	@Override
	public IntWritable getCurrentKey() throws IOException,
			InterruptedException {
//	    if (current == NOT_STARTED || current == DONE) {
//	        return null;
//	    }
//		return new IntWritable(current);
	    return new IntWritable(startPos);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
//	    try {
	        //XMLStreamReader reader = rs.getItemAsStream();
			//return new Text(reader.getText());
	        //return new Text(rs.getItemAsString(null));
	        return new Text(xquery);
//		} catch (XQException e) {
//			throw new IOException(e);
//		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (current == NOT_STARTED) return 0;
	    if (current == DONE) return 1;
	    //return (current-first+1)/(float)total;
	    return 1;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext ctxt)
			throws IOException, InterruptedException {
		
	    readConfiguration(ctxt.getConfiguration());
		
		VPInputSplit vpSplit = (VPInputSplit) split;
		
		xquery = vpSplit.getFragmentQuery();
		startPos = vpSplit.getStartPosition();
//		total = vpSplit.getLengh();
//		selectionLevel = vpSplit.getSelectionLevel();
		
		LOG.debug("xquery: " + xquery);
        //LOG.trace("initialize() from " + first + " to " + (first + total - 1));
		
		//parallelProcessingInit();
		
		//readData();
	}

	private void readConfiguration(Configuration conf) throws IOException {

	    // the subquery for this fragment
	    //xquery = conf.get(VPConst.DB_XQUERY);
	    
	    // configure catalog for this task tracker
		Catalog.get().setConfiguration(conf);
	}

	private void readData() throws IOException {
	    
	    LOG.debug("readData() xquery: " + xquery);

        result = parallelProcessingRun();
	}
	
	private void parallelProcessingInit() throws IOException {
	    SubQuery.getUniqueInstance(true);
	}
	
	/**
	 * From SVP code (adapted) 
	 */
	private String parallelProcessingRun() throws IOException {
	    
	    LOG.debug("VPRecordReader - consulta: " + xquery);
	    
	    String result = "";
	            
	    String query = "";  

        Query q = Query.getUniqueInstance(true);
        long startTime; 
        long delay;
        long tmp;  
        StringReader sr = new StringReader(xquery);
        BufferedReader buff = new BufferedReader(sr);
        String line;
        
        while((line = buff.readLine()) != null){    
            if (!line.toUpperCase().contains("<ORDERBY>") && !line.toUpperCase().contains("<ORDERBYTYPE>") 
                    && !line.toUpperCase().contains("<AGRFUNC>")) {                         
                query = query + " " + line;
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
                        
        startTime = System.nanoTime(); // inicializa o contador de tempo.   
        
        //result = SubQuery.executeSubQuery(query);
            
//            // tempo de leitura de arquivo + execução da consulta
//            delay = ((System.nanoTime() - startTime)/1000000);
//            
//            // tempo das outras sub-consultas já executadas.
//            tmp = this.sbq.getStartTime();                  
//            
//            // soma com o tempo gasto para execução da sub-consulta atual.
//            tmp = tmp + delay; 
//            
//            // atualiza variável
//            q.settotalExecutionTime(q.gettotalExecutionTime() + delay);
//            this.sbq.setStartTime(tmp);
//            
//            try {
//                Thread.sleep(2000);  // tempo especificado em milissegundos                    
//            } catch(InterruptedException e) {}
            
                
//        } catch (FileNotFoundException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        finally {
//            try {
//                if (xqr!=null) xqr.close();
//                if (xqe!=null) xqe.close();
//                if (xqc!=null) xqc.close();
//            } catch (Exception e2) {
//                // TODO: handle exception
//            }
//        }
        return result;
    }

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
        if (current == DONE) {
            return false;
        }
        current = DONE;
        return true;
	}
}

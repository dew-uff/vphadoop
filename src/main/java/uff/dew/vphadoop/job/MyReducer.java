package uff.dew.vphadoop.job;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Set;

import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.fragmentacaoVirtualSimples.SubQuery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import uff.dew.vphadoop.db.Database;
import uff.dew.vphadoop.db.DatabaseFactory;

public class MyReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

    private static final Log LOG = LogFactory.getLog(MyReducer.class);
    
    private static final String TEMP_COLLECTION_NAME = "tmpResultadosParciais";
    
    public MyReducer() {

    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> partials,
            Context context)
            throws IOException, InterruptedException {

        long startTimestamp = System.currentTimeMillis();
        
		// construct db object from configuration file 
        Configuration conf = context.getConfiguration();
		try {
		    DatabaseFactory.produceSingletonDatabaseObject(conf);
		}
		catch(Exception e) {
		    throw new IOException("Something went wrong while reading database configuration values");
		}
		
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        
        // put every partial result in a temp collection at the database
        loadPartialsIntoDatabase(db, partials, context);
        
        long loadingTimestamp = System.currentTimeMillis();
        long dbLoadingTime = (loadingTimestamp - startTimestamp);
        LOG.debug("VP:reducer:tempDBLoadingTime: " + dbLoadingTime + " ms.");
        context.getCounter(VPCounters.COMPOSING_TIME_TEMP_COLLECTION_CREATION).increment(dbLoadingTime);
        
        // the constructFinalQuery (below) was originally executed using the same context
        // previously used to retrieve a partial result (at the coordinator node). 
        // That means that some singletons objects were populated when compiling the final 
        // result. Now we don't have that information, so we need to restore it.
        repopulateQueryAndSubQueryObjects(conf);      

        long repopulateTimestamp = System.currentTimeMillis();
        
        long repopulateTime = repopulateTimestamp - loadingTimestamp;
        LOG.debug("VP:reducer:repopulate: " + repopulateTime + " ms.");
        context.getCounter(VPCounters.COMPOSING_TIME_REPOPULATE_OBJECTS).increment(repopulateTime);
        
        SubQuery sbq = SubQuery.getUniqueInstance(true);
        
        Path path = new Path("result.xml");
        FileSystem fs = FileSystem.get(conf);
        OutputStream resultWriter = fs.create(path);

        // construct the query to get the result from the temp collection
        String finalQuery = constructFinalQuery();
        LOG.debug("Final Query: " + finalQuery);
        
        // execute the final query

        String header = sbq.getConstructorElement() + "\r\n";
        resultWriter.write(header.getBytes());
        
        try {
            XQResultSequence rs = db.executeQuery(finalQuery);
            while (rs.next()) {
                String item = rs.getItemAsString(null);
                resultWriter.write(item.getBytes());
                resultWriter.write("\r\n".getBytes());
                resultWriter.flush();
            }
            db.freeResources(rs);
        }
        catch (XQException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        
        String footer = sbq.getConstructorElement().replace("<", "</");
        resultWriter.write(footer.getBytes());
            
        long reduceQueryTimestamp = System.currentTimeMillis();
        
        long queryExecutionTime = (reduceQueryTimestamp - repopulateTimestamp);
        LOG.debug("VP:reducer:query execution total time: " + queryExecutionTime + " ms.");
        context.getCounter(VPCounters.COMPOSING_TIME_TEMP_COLLECTION_QUERY_EXEC).increment(queryExecutionTime);
        context.getCounter(VPCounters.COMPOSING_TIME).increment(dbLoadingTime + queryExecutionTime);
        
        resultWriter.flush();
        resultWriter.close();
    }
    
    // TODO HACK
    private void repopulateQueryAndSubQueryObjects(Configuration conf) throws IOException {

        String query = "";
        Query q = Query.getUniqueInstance(true);
        SubQuery sbq = SubQuery.getUniqueInstance(true);
        FileSystem fs = FileSystem.get(conf);
        InputStream file = fs.open(new Path("hack.txt"));
        
        InputStreamReader reader = new InputStreamReader(file);
        BufferedReader buff = new BufferedReader(reader);
        
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
                    LOG.debug("hack! order by: " + q.getOrderBy());
                }
                
                if (line.toUpperCase().contains("<ORDERBYTYPE>")){
                    String orderByType= line.substring(line.indexOf("<ORDERBYTYPE>")+"<ORDERBYTYPE>".length(), line.indexOf("</ORDERBYTYPE>"));                         
                    q.setOrderByType(orderByType);
                    LOG.debug("hack! order by type: " + q.getOrderByType());
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
                                    LOG.debug("hack! aggr function: " + hashParts[0] + ":" + hashParts[1]);
                                }
                            }
                            
                        }
                    }                       
                }                       
            }
        }
        buff.close();
        reader.close();
        file.close();
        
        LOG.debug("hack! query: " + query);

        sbq.setConstructorElement(SubQuery.getConstructorElement(query));
        sbq.setElementAfterConstructor(SubQuery.getElementAfterConstructor(query));

        LOG.debug("hack! constructor element: " + sbq.getConstructorElement());
        LOG.debug("hack! element after constructor: " + sbq.getElementAfterConstructor());
    }

    private void loadPartialsIntoDatabase(Database db, Iterable<Text> partials, Context context) throws IOException {
        try {
        	
        	long timestamp = System.currentTimeMillis();
        	
        	db.createCollection(TEMP_COLLECTION_NAME);
        	
            int count = 0;
            for(Text partial : partials) {
                String docName = "partial_"+ count++ +".xml";
                LOG.debug("Creating document " + docName + " into collection " + TEMP_COLLECTION_NAME);
                ByteArrayInputStream bais = new ByteArrayInputStream(partial.getBytes(),0,partial.getLength());
                db.addDocumentToCollection(bais,docName,TEMP_COLLECTION_NAME);
                LOG.debug("Creation suceeded!");
                bais.close();
            }

            LOG.debug("loadPartialsIntoDb:time to create temp collection: " + (System.currentTimeMillis() - timestamp) + " ms.");
            timestamp = System.currentTimeMillis();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }
    
    /**
     * From FinalResult.getFinalResult (adapted)
     * 
     * @return
     * @throws IOException
     */
    private String constructFinalQuery() throws IOException {
        
        Query q = Query.getUniqueInstance(true);
        SubQuery sbq = SubQuery.getUniqueInstance(true);

        String finalResultXquery = "";
        String orderByClause = "";
        String variableName = "$ret";



        // possui funcoes de agregacao na clausula LET.
        if (q.getAggregateFunctions() != null
                && q.getAggregateFunctions().size() > 0) { 

            finalResultXquery = 
                    " \r\n"
                    + " let $c:= collection('"+TEMP_COLLECTION_NAME+"')/partialResult/"
                    + sbq.getConstructorElement().replaceAll("[</>]", "")
                    + "\r\n"
                    // + " where $c/element()/name()!='idOrdem'"
                    + " \r\n return \r\n\t" + "<"
                    + sbq.getElementAfterConstructor().replaceAll("[</>]", "")
                    + ">";

            Set<String> keys = q.getAggregateFunctions().keySet();
            for (String function : keys) {
                String expression = q.getAggregateFunctions().get(function);
                String elementsAroundFunction = "";

                if (expression.indexOf(":") != -1) {
                    elementsAroundFunction = expression.substring(
                            expression.indexOf(":") + 1, expression.length());
                    expression = expression.substring(0,
                            expression.indexOf(":"));
                }

                // o elemento depois do return possui sub-elementos.
                if (elementsAroundFunction.indexOf("/") != -1) { 
                    // elementsAroundFunction =
                    // elementsAroundFunction.substring(elementsAroundFunction.indexOf("/")+1,
                    // elementsAroundFunction.length());
                    String[] elm = elementsAroundFunction.split("/");

                    for (String openElement : elm) {
                        // System.out.println("FinalResult.getFinalResult():"+elm.length+","+sbq.getElementAfterConstructor().replaceAll("[</>]",
                        // "") +",el:"+openElement);

                        if (!openElement.equals("") &&
                            !openElement.equals(sbq
                                        .getElementAfterConstructor()
                                        .replaceAll("[</>]", ""))) {
                            // System.out.println("FinalResult.getFinalResult(); armazenar o el:::"+openElement);
                            finalResultXquery = finalResultXquery + "\r\n\t\t"
                                    + " <" + openElement + ">";
                        }
                    }

                    elm = elementsAroundFunction.split("/");
                    String subExpression = expression.substring(
                            expression.indexOf("$"), expression.length());
                    // System.out.println("FinalResult.getFinalResult(); subExpression o el:::"+subExpression);

                    if (subExpression.indexOf("/") != -1) { // agregacao com
                                                            // caminho xpath.
                                                            // Ex.:
                                                            // count($c/total)
                        subExpression = subExpression.substring(
                                subExpression.indexOf("/") + 1,
                                subExpression.length());
                        // System.out.println("FinalResult.getFinalResult(); depois alterar o el:::"+subExpression+",el:"+elementsAroundFunction);
                        expression = expression.replace("$c/" + subExpression,
                                "$c/" + elementsAroundFunction + ")");

                        // System.out.println("FinalResult.getFinalResult(); depois alterar o expression:::"+expression);

                    } else { // agregacao sem caminho xpath. Ex.: count($c)
                        expression = expression.replace("$c", "$c/"
                                + elementsAroundFunction);
                    }

                    if (expression.indexOf("count(") >= 0) {
                        expression = expression.replace("count(", "sum("); // pois
                                                                            // deve-se
                                                                            // somar
                                                                            // os
                                                                            // valores
                                                                            // já
                                                                            // previamente
                                                                            // computados
                                                                            // nos
                                                                            // resultados
                                                                            // parciais.
                    }

                    finalResultXquery = finalResultXquery + "{ " + expression
                            + "}";

                    for (int i = elm.length - 1; i >= 0; i--) {
                        String closeElement = elm[i];

                        if (!closeElement.equals("")
                                && !closeElement.equals(sbq
                                        .getElementAfterConstructor()
                                        .replaceAll("[</>]", ""))) {
                            // System.out.println("FinalResult.getFinalResult(); armazenar o el:::"+closeElement);
                            finalResultXquery = finalResultXquery + "\r\n\t\t"
                                    + " </" + closeElement + ">";
                        }
                    }

                } else { // apos o elemento depois do return estah a funcao de
                            // agregacao. ex.: return <resp> count($c) </resp>
                    elementsAroundFunction = "";
                    expression = expression.replace(
                            "$c)",
                            "$c/"
                                    + sbq.getElementAfterConstructor().replaceAll(
                                            "[</>]", "") + ")");
                    // System.out.println("FinalResult.getFinalResult(); entrei!!!!!!!!!!!"+expression+","+sbq.getElementAfterConstructor());

                    String subExpression = expression.substring(
                            expression.indexOf("$"), expression.length());

                    if (subExpression.indexOf("/") != -1) { // agregacao com
                                                            // caminho xpath.
                                                            // Ex.:
                                                            // count($c/total)
                        subExpression = subExpression.substring(
                                subExpression.indexOf("/") + 1,
                                subExpression.length());
                        // System.out.println("FinalResult.getFinalResult(); depois alterar o el:::"+subExpression+",el:"+elementsAroundFunction);
                        expression = expression.replace(
                                "$c/" + subExpression,
                                "$c/"
                                        + sbq.getElementAfterConstructor()
                                                .replaceAll("[</>]", "") + ")");

                        // System.out.println("FinalResult.getFinalResult(); depois alterar o expression:::"+expression);

                    } else { // agregacao sem caminho xpath. Ex.: count($c)
                        expression = expression.replace(
                                "$c",
                                "$c/"
                                        + sbq.getElementAfterConstructor()
                                                .replaceAll("[</>]", ""));
                    }

                    if (expression.indexOf("count(") >= 0) {
                        expression = expression.replace("count(", "sum("); // pois
                                                                            // deve-se
                                                                            // somar
                                                                            // os
                                                                            // valores
                                                                            // já
                                                                            // previamente
                                                                            // computados
                                                                            // nos
                                                                            // resultados
                                                                            // parciais.
                    }

                    finalResultXquery = finalResultXquery + "{ " + expression
                            + "}";
                }

                /*
                 * System.out.println("FinalResult.getFinalResult(), EXPRESSION:"
                 * +expression + ", ELEROUND:"+elementsAroundFunction);
                 * finalResultXquery = finalResultXquery + "\r\n\t\t" + "{ <" +
                 * elementsAroundFunction + ">" + "\r\n\t" + expression + "} </"
                 * + elementsAroundFunction + "> ";
                 */

            } // fim for

            finalResultXquery = finalResultXquery + "\r\n\t"
                    + sbq.getElementAfterConstructor().replace("<", "</")
                    ;

            // System.out.println("FinalResult.getFinalResult(): consulta final eh:"+finalResultXquery);

        }

        else if (!q.getOrderBy().trim().equals("")) { // se a consulta original
                                                        // possui order by,
                                                        // acrescentar na
                                                        // consulta final o
                                                        // order by original.

            String[] orderElements = q.getOrderBy().trim().split("\\$");
            for (int i = 0; i < orderElements.length; i++) {
                String subOrder = ""; // caminho apos a definicao da variavel.
                                        // Ex.: $order/shipdate. subOrder recebe
                                        // shipdate.
                int posSlash = orderElements[i].trim().indexOf("/");

                if (posSlash != -1) {
                    subOrder = orderElements[i].trim().substring(posSlash + 1,
                            orderElements[i].length());
                    if (subOrder.charAt(subOrder.length() - 1) == '/') {
                        subOrder = subOrder.substring(0, subOrder.length() - 1);
                    }
                }

                if (!subOrder.equals("")) {
                	orderByClause = orderByClause + (orderByClause.equals("")?"": ", ") + variableName + "/key/" + subOrder;
                }
            }

            finalResultXquery = 
                    " for $ret in collection('"+TEMP_COLLECTION_NAME+"')/partialResult/"
                    + sbq.getConstructorElement().replaceAll("[</>]", "") + "/"
                    + "orderby"
                    + " order by " + orderByClause + " return $ret/element/" 
                    + sbq.getElementAfterConstructor().replaceAll("[</>]", "") 
                    ;

            // System.out.println("finalresult.java:"+ finalResultXquery);
        } else { // se a consulta original nao possui order by, acrescentar na
                    // consulta final a ordenacao de acordo com a ordem dos
                    // elementos nos documentos pesquisados.
            orderByClause = "number($ret/idOrdem)";

            finalResultXquery = 
                    
                    " for $ret in collection('"+TEMP_COLLECTION_NAME+"')/partialResult"
                    + " let $c:= $ret/"
                    + sbq.getConstructorElement().replaceAll("[</>]", "")
                    + "/element()" // where $ret/element()/name()!='idOrdem'"
                    + " order by " + orderByClause + " ascending"
                    + " return $c" 
                    ;

            // System.out.println("finalresult.java:"+ finalResultXquery);
        }
        
        return finalResultXquery;
    }
}
package uff.dew.vphadoop.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
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

import uff.dew.vphadoop.db.Catalog;
import uff.dew.vphadoop.db.Database;

public class MyReducer extends Reducer<NullWritable, Text, Text, NullWritable> {

    private static final Log LOG = LogFactory.getLog(MyReducer.class);
    
    private static final String TEMP_COLLECTION_NAME = "tmpResultadosParciais";
    
    // HACK
    private String retorno = "";
    
    public MyReducer() {

    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException {
        
        // put every partial result in a temp collection at the database
        Configuration conf = context.getConfiguration();
        Catalog.get().setConfiguration(conf);
        Database db = Catalog.get().getDatabase();
        
        long startTimestamp = System.currentTimeMillis();
        
        loadPartialsIntoDatabase(db, values, context);
        
        long loadingTimestamp = System.currentTimeMillis();
        
        LOG.debug("VP:reducer:tempDBLoadingTime: " + (loadingTimestamp - startTimestamp) + " ms.");
        
        // the constructFinalQuery (below) was originally executed using the same context
        // previously used to retrieve a partial result (at the coordinator node). 
        // That means that some singletons objects were populated when compiling the final 
        // result. Now we don't have that information, so we need to restore it.
        repopulateQueryAndSubQueryObjects(conf);      

        long repopulateTimestamp = System.currentTimeMillis();
        
        LOG.debug("VP:reducer:repopulate: " + (repopulateTimestamp - loadingTimestamp) + " ms.");
        
        SubQuery sbq = SubQuery.getUniqueInstance(true);
        
        Path path = new Path("result.xml");
        FileSystem fs = FileSystem.get(conf);
        OutputStream resultWriter = fs.create(path);

        // if retorno contains just the constructor, means that all the partial results were 
        // empty, so we don't have to run the query.
        if (retorno.lastIndexOf('<') != 0) {
        
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
                    resultWriter.flush();
                }
            }
            catch (XQException e) {
                e.printStackTrace();
                throw new IOException(e);
            }
            
            String footer = sbq.getConstructorElement().replace("<", "</");
            resultWriter.write(footer.getBytes());
            
        } else {
            // if retorno contains only the the element constructor then that's the result
            resultWriter.write(retorno.getBytes());
        }
       
        long reduceQueryTimestamp = System.currentTimeMillis();
        
        LOG.debug("VP:reducer:query: " + (reduceQueryTimestamp - repopulateTimestamp) + " ms.");

        resultWriter.flush();
        resultWriter.close();
    }
    
    // HACK
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
        
        if (retorno.lastIndexOf('<') != 0) {
            // means that at least one partial result got anything more than just the constructor
            
            if (retorno.indexOf("<idOrdem>") != -1 )
                retorno = retorno.substring(retorno.indexOf("<partialResult>")+"<partialResult>".length(), retorno.indexOf("<idOrdem>"));
            else
                retorno = retorno.substring(retorno.indexOf("<partialResult>")+"<partialResult>".length(), retorno.indexOf("</partialResult>"));
            
            sbq.setConstructorElement(SubQuery.getConstructorElement(retorno)); // Usado para a composicao do resultado final.
            
            //String intervalBeginning = SubQuery.getIntervalBeginning(xquery);
            
            if ( sbq.getElementAfterConstructor().equals("") ) {
                sbq.setElementAfterConstructor(SubQuery.getElementAfterConstructorElement(retorno, sbq.getConstructorElement()));
            }
            
            if (sbq.isUpdateOrderClause()) {
                SubQuery.getElementsAroundOrderByElement(query, sbq.getElementAfterConstructor());
            }
        } else {
            sbq.setConstructorElement(SubQuery.getConstructorElement(retorno)); // setting just the constructor element to
        }
    }

    private void loadPartialsIntoDatabase(Database db, Iterable<Text> values, Context context) throws IOException {
        try {
            db.deleteCollection(TEMP_COLLECTION_NAME);
            File tempDir = createTempDirectory();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            int count = 0;
            for(Text filename : values) {
                String localFilename = tempDir.getAbsolutePath()+"/partial_"+ count++ +".xml";
                fs.copyToLocalFile(new Path(filename.toString()), new Path(localFilename));
            }

            if (tempDir.list().length > 0) {
                hackSetRetornoVariable(tempDir);
                db.createCollectionWithContent(TEMP_COLLECTION_NAME, tempDir.getAbsolutePath());
            }
            
            deleteTempDirectory(tempDir);
            
        } catch (XQException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }
    
    private void hackSetRetornoVariable(File dir) {
        File[] partials = dir.listFiles(new FileFilter() {
            
            @Override
            public boolean accept(File file) {
                if (file.getName().endsWith(".xml")){
                    return true;
                } else {
                    return false;
                }
            }
        });
        
        for (File f : partials) {
            try {
                String temp = readFileContent(f.getAbsolutePath());
                if (retorno.length() == 0 || 
                        (retorno.lastIndexOf('<') == 0 && temp.lastIndexOf('<') != 0)) {
                    retorno = temp;
                }
                if (retorno.lastIndexOf('<') != 0) {
                    break;
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String readFileContent(String filename) throws FileNotFoundException, IOException {
        
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
                    orderByClause = orderByClause
                            + (orderByClause.equals("") ? "" : ", ")
                            + variableName + "/" + subOrder;
                }
            }

            finalResultXquery = 
                    " for $ret in collection('"+TEMP_COLLECTION_NAME+"')/partialResult/"
                    + sbq.getConstructorElement().replaceAll("[</>]", "") + "/"
                    + sbq.getElementAfterConstructor().replaceAll("[</>]", "")
                    + " order by " + orderByClause + " return $ret" 
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
        
        
    private static File createTempDirectory() throws IOException {
        final File temp;

        temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if(!(temp.delete()))
        {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if(!(temp.mkdir()))
        {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        return (temp);
    }
    
    private static void deleteTempDirectory(File tempDir) throws IOException {
        
        File[] innerFiles = tempDir.listFiles();
        for(File f : innerFiles) {
            f.delete();
        }
        
        tempDir.delete();
    }
}
package uff.dew.svp;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;

import uff.dew.svp.catalog.Catalog;
import uff.dew.svp.db.DatabaseException;
import uff.dew.svp.db.DatabaseFactory;
import uff.dew.svp.fragmentacaoVirtualSimples.DecomposeQuery;
import uff.dew.svp.fragmentacaoVirtualSimples.Query;
import uff.dew.svp.fragmentacaoVirtualSimples.SimpleVirtualPartitioning;
import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;

/**
 * Object to execute a subquery originated from SVP technique
 * 
 * @author gabriel
 *
 */
public class SubQueryExecutor {
    
    private String subQuery;
    private ExecutionContext context;
    
    /**
     * Creates a Executor object using the fragment originated in SVP technique
     * 
     * @param fragment
     * @throws Exception
     */
    public SubQueryExecutor(String fragment) throws SubQueryExecutionException {
        cleanSingletons();
        try {
            subQuery = processFragment(fragment);
            if (subQuery.indexOf("order by") != -1) {
                subQuery = insertOrderByElementInSubQuery(subQuery);
                subQuery = removeOrderByFromSubquery(subQuery);
            }
            
            context = new ExecutionContext(fragment);
        }
        catch (IOException e) {
            throw new SubQueryExecutionException(e);
        }
    }
    
    private void cleanSingletons() {
        // need to do this to get rid of garbage from previous execution
        Query.getUniqueInstance(false);
        SimpleVirtualPartitioning.getUniqueInstance(false);
        SubQuery.getUniqueInstance(false);
        DecomposeQuery.getUniqueInstance(false);        
    }

    /**
     * Sets the local database information where the subquery will be executed
     * 
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @param databaseName
     * @param type
     * @throws DatabaseException
     */
    public void setDatabaseInfo(String hostname, int port, String username, String password,
            String databaseName, String type) throws DatabaseException {
        DatabaseFactory.produceSingletonDatabaseObject(hostname,port,username,
                password,databaseName,type);
        Catalog.get().setDatabaseObject(DatabaseFactory.getSingletonDatabaseObject());
    }
    
    /**
     * Executes the subquery saving the results in the given OutputStream
     * 
     * @param os
     * @throws Exception
     */
    public boolean executeQuery(OutputStream os) throws SubQueryExecutionException {
        
        return SubQuery.executeSubQuery(subQuery, os);
    }
    
    /**
     * Returns the execution context of this query
     * @return the execution context
     */
    public ExecutionContext getExecutionContext() {
        return context;
    }
    
    private String processFragment(String fragment) throws IOException {

        Query q = Query.getUniqueInstance(true);
        SubQuery sbq = SubQuery.getUniqueInstance(true);
        
        StringReader sr = new StringReader(fragment);
        BufferedReader buff = new BufferedReader(sr);

        String line;
        String subquery = "";
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
        return subquery;
    }
    
    private String insertOrderByElementInSubQuery(String subquery) {
        String orderByElement = getOrderByElementFromQuery(subquery);
        
        String beginElement = SubQuery.getElementAfterConstructor(subquery); // <element>
        String endElement = beginElement.replace("<", "</");
        
        int beginInsertPos = subquery.indexOf(beginElement);
        int endInsertPos = subquery.indexOf(endElement)+endElement.length();
        
        String wholeElement = subquery.substring(beginInsertPos,endInsertPos);
        
        subquery = subquery.substring(0,beginInsertPos) + "\r\n"
                + "<orderby>"
                + "<key>{"+orderByElement+"}</key>\r\n"
                + "<element>" + wholeElement + "</element>\r\n"
                + "</orderby>" +
                subquery.substring(endInsertPos);

        return subquery;
    }

    private static String getOrderByElementFromQuery(String query) {
        String orderBy = query;
        int orderByPos = orderBy.indexOf("order by") + "order by".length();
        orderBy = orderBy.substring(orderByPos).trim();
        int returnPos = orderBy.indexOf("return");
        orderBy = orderBy.substring(0, returnPos).trim();
        
        if (orderBy.indexOf("ascending") != -1) {
            int ascPos = orderBy.indexOf("ascending");
            orderBy = orderBy.substring(0,ascPos).trim();
        } else if (orderBy.indexOf("descending") != -1) {
            int descPos = orderBy.indexOf("descending");
            orderBy = orderBy.substring(0,descPos).trim();          
        }
        
        return orderBy;
    }
    
    private String removeOrderByFromSubquery(String query) {
        String orderBy = query;
        int orderByPos = orderBy.indexOf("order by");
        orderBy = orderBy.substring(orderByPos).trim();
        int returnPos = orderBy.indexOf("return");
        orderBy = orderBy.substring(0, returnPos).trim();
        
        return query.replace(orderBy, "");
    }
}

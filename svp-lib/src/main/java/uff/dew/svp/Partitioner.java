package uff.dew.svp;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import uff.dew.svp.catalog.Catalog;
import uff.dew.svp.db.DatabaseException;
import uff.dew.svp.db.DatabaseFactory;
import uff.dew.svp.engine.XQueryEngine;
import uff.dew.svp.engine.XQueryEngineException;
import uff.dew.svp.exceptions.PartitioningException;
import uff.dew.svp.fragmentacaoVirtualSimples.DecomposeQuery;
import uff.dew.svp.fragmentacaoVirtualSimples.ExistsJoinOperation;
import uff.dew.svp.fragmentacaoVirtualSimples.Query;
import uff.dew.svp.fragmentacaoVirtualSimples.SimpleVirtualPartitioning;
import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;

/**
 * User class to access Simple Virtual Partitioning
 * functionality. 
 *
 */
final public class Partitioner 
{
    private ArrayList<String> docQueries;
    private ArrayList<String> docQueriesWithoutFragmentation;
    private String originalQuery;
    private String inputQuery;
    private int nfragments = 50;
    private String xquery;
    private XQueryEngine engine;
    
    private ExecutionContext context;
    
    /**
     * Creates a Partitioner object that uses catalog strategy to execute fragmentation
     * 
     * @param catalogStream the InputStream object to the file containing the catalog
     */
    public Partitioner(InputStream catalogStream)
    {
        Catalog.get().populateCatalogFromFile(catalogStream);
        cleanSingletons();
    }
    
    /**
     * Creates a Partitioner object that uses database strategy to execute fragmentation
     * It will pose several queries to database to determine cardinality of elements
     * 
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @param databaseName
     * @param dbType
     * @throws DatabaseException
     */
    public Partitioner(String hostname, int port, String username, String password, 
            String databaseName, String dbType) throws DatabaseException {
        DatabaseFactory.produceSingletonDatabaseObject(hostname,port,username,
                password,databaseName,dbType);
        Catalog.get().setDatabaseObject(DatabaseFactory.getSingletonDatabaseObject());
        Catalog.get().setDbMode(true);
        cleanSingletons();
    }
    
    private void cleanSingletons() {
        // need to do this to get rid of garbage from previous execution
        Query.getUniqueInstance(false);
        SimpleVirtualPartitioning.getUniqueInstance(false);
        SubQuery.getUniqueInstance(false);
        DecomposeQuery.getUniqueInstance(false);        
    }
    
    public List<String> executePartitioning(String query, int nFragments) 
            throws PartitioningException {
        
        inputQuery = query;
        nfragments = nFragments;
        
        // to mimic PartiX-VP flow.
        svpPressed();
        
        List<String> fragments = getFragments();
        
        if (fragments == null) {
            throw new PartitioningException("queries should not be null!");
        }
        
        context = new ExecutionContext(fragments.get(0));
        
        return fragments;
    }
    
    public ExecutionContext getExecutionContext() {
        return context;
    }
    
    private void xqueryPressed() throws PartitioningException {
        
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
           throw new PartitioningException("Erro ao gerar sub-consultas para a coleção indicada. Verifique a consulta de entrada.");
        }
    }

    private void svpPressed() throws PartitioningException {
        
        xqueryPressed();
        
        try {
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
        catch (XQueryEngineException e) {
            throw new PartitioningException(e);
        }
    }
    

    private void executeXQuery() throws XQueryEngineException {
        
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
    
    private List<String> getFragments() {
        
        Query q = Query.getUniqueInstance(true);
        SubQuery sbq = SubQuery.getUniqueInstance(true);       
        
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

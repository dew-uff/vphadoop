package uff.dew.svp.fragmentacaoVirtualSimples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import uff.dew.svp.catalog.CatalogManager;

public class Query {

	protected String queryExprType; // Indica se a consulta informada pelo usurio utiliza a clusula doc() ou a clusula collection();
	protected String fragmentationAttribute; // Indica o caminho xpath do atributo de fragmentao virtual	
	protected String virtualPartitioningVariable; // Indicao da varivel de fragmentao, nos casos em que existem junes
	protected int verifiedJoins;
	protected int lastJoinCardinality; // A cardinalidade da juno, quando h mais de um For com doc() clauses 
	protected boolean joinCheckingFinished; // indica se a verificao dos joins j acabou
	protected int lastCollectionCardinality; // A cardinalidade da juno, quando h mais de um For, porm com collection() clauses
	protected String ancestralPath = ""; // caminho completo at o nodo pai do elemento especificado na consulta. Ex: doc()//element;
	private boolean existsJoin;
	
	public boolean isExistsJoin() {
		return existsJoin;
	}

	public void setExistsJoin(boolean existsJoin) {
		this.existsJoin = existsJoin;
	}

	public String getAncestralPath() {
		return ancestralPath;
	}

	public void setAncestralPath(String ancestralPath) {
		this.ancestralPath = ancestralPath;
	}

	public String getPartitioningPath() {
		return partitioningPath;
	}

	public void setPartitioningPath(String partitioningPath) {
		this.partitioningPath = partitioningPath;
	}

	protected String partitioningPath; // O caminho at o atributo de fragmentao, quando h mais de um For, porm com collection() clauses
	
	public int getLastCollectionCardinality() {
		return lastCollectionCardinality;
	}

	public void setLastCollectionCardinality(int lastCollectionCardinality) {
		this.lastCollectionCardinality = lastCollectionCardinality;
	}

	public boolean isJoinCheckingFinished() {
		return joinCheckingFinished;
	}

	public void setJoinCheckingFinished(boolean joinCheckingFinished) {
		this.joinCheckingFinished = joinCheckingFinished;
	}

	public int getLastJoinCardinality() {
		return lastJoinCardinality;
	}

	public void setLastJoinCardinality(int lastJoinCardinality) {
		this.lastJoinCardinality = lastJoinCardinality;
	}

	public int getVerifiedJoins() {
		return verifiedJoins;
	}

	public void setVerifiedJoins(int verifiedJoins) {
		this.verifiedJoins = verifiedJoins;
	}

	public String getVirtualPartitioningVariable() {
		return virtualPartitioningVariable;
	}

	public void setVirtualPartitioningVariable(String virtualPartitioningVariable) {
		this.virtualPartitioningVariable = virtualPartitioningVariable;
	}

	protected String xpath="";
	protected String xpathAggregateFunction = "";
	
	public String getXpathAggregateFunction() {
		return xpathAggregateFunction;
	}

	public void setXpathAggregateFunction(String xpathAggregateFunction) {
		this.xpathAggregateFunction = xpathAggregateFunction;
	}

	protected String inputQuery=""; // Armazena a consulta de entrada
	protected String orderBy = "";
	protected String orderByType = ""; // indica se a ordenacao  ascending ou descending.
	protected String lastReadForLetVariable = "";
	protected String lastReturnVariable = "";
	protected String lastReadFunction = ""; // indica a ultima funcao de agregacao lida
	protected boolean isWaitingXpathAggregateFunction = false; // indica se esta esperando a leitura do caminho xpath referente a funcao de agregacao. Ex.: count($l/order_line). Estaria aguardando a leitura de order_line.
	protected String elementsAroundFunction = ""; // indica os elementos em torno da funcao de agregacao. Ex.: ... return <resp><total><arrecadacao>{sum($pagto)}</arrecadacao></total><resp>. ElementsAroundFunction = resp|total|arrecacadao.
	
	public String getElementsAroundFunction() {
		return elementsAroundFunction;
	}

	public void setElementsAroundFunction(String elementsAroundFunction) {
		this.elementsAroundFunction = elementsAroundFunction;
	}

	public String getLastReadFunction() {
		return lastReadFunction;
	}

	public void setLastReadFunction(String lastReadFunction) {
		this.lastReadFunction = lastReadFunction;
	}
	
	public void setAggregateFunc(String variableName, String aggregateFunction) {
		if ( this.aggregateFunctions == null ){
			this.aggregateFunctions = new Hashtable<String, String>();
		}
		
		this.aggregateFunctions.put(variableName, aggregateFunction);
	}

	
	public boolean isWaitingXpathAggregateFunction() {
		return isWaitingXpathAggregateFunction;
	}

	public void setWaitingXpathAggregateFunction(boolean isWaitingXpathAggregateFunction) {
		this.isWaitingXpathAggregateFunction = isWaitingXpathAggregateFunction;
	}

	public String getLastReturnVariable() {
		return lastReturnVariable;
	}

	public void setLastReturnVariable(String lastReturnVariable) {
		this.lastReturnVariable = lastReturnVariable;
	}

    protected Hashtable<String, String> aggregateFunctions; // indica as funoes de agregao que devem ser acrescentadas no final.
	
	public Hashtable<String, String> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public void setAggregateFunctions(String aggregateFunction, String variableName, String comparisonOp) {
		
		if (this.aggregateFunctions == null) {
			this.aggregateFunctions = new Hashtable<String, String>();
		}
		
		// verifica qual a funcao utilizada
		String functionPredicate = "";
		
		functionPredicate = aggregateFunction + "(" + variableName + ")" + comparisonOp;
		
		if ( comparisonOp!=null && !comparisonOp.equals("") ) { // count de clasulas Where em consultas com FOR									
			this.getAggregateReturn().put(variableName, functionPredicate);
		}
		else { // count em consultas com LET.			
			this.getAggregateFunctions().put(functionPredicate, functionPredicate);
		}
	}

	public String getLastReadForLetVariable() {
		return lastReadForLetVariable;
	}

	public void setLastReadForLetVariable(String lastReadForLetVariable) {
		this.lastReadForLetVariable = lastReadForLetVariable;
	}

	public String getOrderByType() {
		return orderByType;
	}

	public void setOrderByType(String orderByType) {
		this.orderByType = orderByType;
	}

	public String getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(String orderBy) {
		this.orderBy = orderBy;
	}

	public String getInputQuery() {
		return inputQuery;
	}

	public void setInputQuery(String inputQuery) {
		this.inputQuery = inputQuery;
	}
	
	protected String fragmentationVariable= ""; // Setada na classe ForLetClause.java ao iniciar o parser de um FOR.
	protected String lastReadLetVariable = "";
	protected String lastReadWhereVariable = "";
	protected String lastReadSimplePathExpr = "";
	protected String lastReadDocumentExpr = "";
	protected String lastReadCollectionExpr = "";
	protected int lastReadCardinality = 0;
	protected boolean addedPredicate;
	protected boolean elementConstructor;
	protected boolean orderByClause;
	
	public boolean isOrderByClause() {
		return orderByClause;
	}

	public void setOrderByClause(boolean orderByClause) {
		this.orderByClause = orderByClause;
	}

	public boolean isElementConstructor() {
		return elementConstructor;
	}

	public void setElementConstructor(boolean elementConstructor) {
		this.elementConstructor = elementConstructor;
	}

	public boolean isAddedPredicate() {
		return addedPredicate;
	}

	public void setAddedPredicate(boolean addedPredicate) {
		this.addedPredicate = addedPredicate;
	}

	public int getLastReadCardinality() {
		return lastReadCardinality;
	}

	public void setLastReadCardinality(int lastReadCardinality) {
		this.lastReadCardinality = lastReadCardinality;
	}

	public String getLastReadCollectionExpr() {
		return lastReadCollectionExpr;
	}

	public void setLastReadCollectionExpr(String lastReadCollectionExpr) {
		this.lastReadCollectionExpr = lastReadCollectionExpr;
	}

	public String getLastReadDocumentExpr() {
		return lastReadDocumentExpr;
	}

	public void setLastReadDocumentExpr(String lastReadDocumentExpr) {
		this.lastReadDocumentExpr = lastReadDocumentExpr;
	}

	protected String operatorComparision = "";	

	protected static Query _inputQuery;	
	
	protected Hashtable<String,String> forClauses = new Hashtable<String,String>();
	protected Hashtable<String,String> letClauses = new Hashtable<String,String>();
	protected Hashtable<String,String> selectionPredicates = new Hashtable<String,String>();
	protected Hashtable<String,String> aggregateReturn = new Hashtable<String,String>(); // indica os nomes do elementos em torno das funcoes de agregacao no resultado.
	
	public Hashtable<String, String> getAggregateReturn() {
		return aggregateReturn;
	}

	public void setAggregateReturn(String variableName, String aggregateFunction) {
		if ( this.aggregateReturn == null ){
			this.aggregateReturn = new Hashtable<String, String>();
		}
		
		this.aggregateReturn.put(variableName, aggregateFunction);
	}

	protected Hashtable<String,Hashtable<String,String>> indexes = new Hashtable<String,Hashtable<String,String>>();
	protected Hashtable<String,String> indexesToBeUsed = new Hashtable<String,String>();
	
	public String getqueryExprType(){
		return queryExprType;
	}
	
	public static Query getUniqueInstance(boolean flag) {		
		if (!flag || _inputQuery == null)
			_inputQuery = new Query();
				
		return _inputQuery;
	}	
	
	
	/***
	 * Este mtodo verifica se a consulta de entrada  aplicada sobre um documento ou sobre uma coleo.
	 * @param inputQuery A consulta de entrada atravs da interface.
	 * @throws IOException 
	 */
	public ArrayList<String> setqueryExprType(String inputQuery) {
				
		int posIn = -1;						
		int posParentese = -1;
		String documentExpr = "";
		String temp = "";
		String collectionName = "";
		CatalogManager cm = CatalogManager.getUniqueInstance();		
		
		int posvar = -1;
			    		
		if (inputQuery!=null && inputQuery.toUpperCase().indexOf("FOR ")>=0) {
			posIn = inputQuery.indexOf("in ");			
			posParentese = inputQuery.indexOf("(");	

			if ( posIn != -1 && posParentese != -1 && posIn > posParentese) { // No faa nada.
				; // A consulta de entrada possui um LET antes de um FOR. Neste sistema, as consultas de entrada ou possuem For clause ou For seguido de Let, mas nunca o oposto.
			}
			else {
				documentExpr = inputQuery.substring(posIn,posParentese);
			}
		} // fim if expresso igual a FOR
		
		else if (inputQuery!=null && inputQuery.toUpperCase().indexOf("LET ")>=0) {
			posIn = inputQuery.indexOf(":=");		
			posParentese = inputQuery.indexOf("(");
			documentExpr = inputQuery.substring(posIn,posParentese);	
		} // fim if expresso igual a LET		
		
		if (documentExpr!=null && documentExpr.toUpperCase().indexOf("DOC")>=0) {			
			queryExprType = "document";
		}
		else if (documentExpr!=null && documentExpr.toUpperCase().indexOf("COLLECTION")>=0){
			queryExprType = "collection";		
			
			temp = inputQuery.substring(inputQuery.toUpperCase().indexOf("COLLECTION("), inputQuery.indexOf(")"));			
			collectionName = temp.substring(temp.indexOf("'")+1,temp.length()-1);			
			DecomposeQuery dq = DecomposeQuery.getUniqueInstance(true);			
			String subInputQuery = inputQuery;		
			
			posvar = subInputQuery.indexOf("$");
			String varName = subInputQuery.substring(posvar, posIn);
			ArrayList<String> subqueries = dq.getSubQueries(cm.getFormattedDocumentsQuery(collectionName), inputQuery, posvar, collectionName, varName);
								
			subInputQuery = subInputQuery.substring(posvar, subInputQuery.length());			
			subInputQuery = subInputQuery.substring(subInputQuery.indexOf(" "), subInputQuery.length());			
			subInputQuery = subInputQuery.substring(subInputQuery.indexOf(collectionName), subInputQuery.length());
			
			while (subInputQuery!=null && subInputQuery.toUpperCase().indexOf("FOR ")!=-1) { // Para os casos em que ha mais de uma clausula for na consulta original.
								
				subInputQuery = subInputQuery.substring(subInputQuery.toUpperCase().indexOf("FOR "), subInputQuery.length());
				posIn = subInputQuery.indexOf("in ");
				posParentese = subInputQuery.indexOf("(");
								
				posvar = subInputQuery.indexOf("$");
				
				varName = subInputQuery.substring(posvar,posIn);
				temp = subInputQuery.substring(subInputQuery.toUpperCase().indexOf("COLLECTION("), subInputQuery.indexOf(")"));
				collectionName = temp.substring(temp.indexOf("'")+1,temp.length()-1);
				subqueries = dq.getSubQueries(cm.getFormattedDocumentsQuery(collectionName), inputQuery, inputQuery.indexOf(varName), collectionName, varName);
				
				subInputQuery = subInputQuery.substring(posvar, subInputQuery.length());				
				subInputQuery = subInputQuery.substring(subInputQuery.indexOf(" "), subInputQuery.length());				
				subInputQuery = subInputQuery.substring(subInputQuery.indexOf(collectionName), subInputQuery.length());				
				
			}
						
			return subqueries;
			
		}
		else {
			queryExprType = null;
			return null;
		}
		
		return null;
	}

	public String getFragmentationAttribute() {
		return fragmentationAttribute;
	}

	public void setFragmentationAttribute(String attributeName) {
		this.fragmentationAttribute = attributeName;
	}
	
	public String getPathVariable(String variableName){
		
		String path = this.forClauses.get(variableName); // caminho da varivel
		int posComma = path.indexOf(":");		
		String subPath = path.substring(posComma+1,path.length());		
		
		if (subPath.indexOf(":")!=-1){			
			subPath = subPath.substring(subPath.indexOf(":")+1,subPath.length());
		}	
		
		if (subPath.equals("")) {
			subPath = path.substring(0,posComma);
		}
		
		return subPath;
	}
			
	
	public String getDocumentNameByVariableName(String variableName) {

		/* this.forClauses  uma hashtable que possuem a seguinte estrutura:
		<chave, conteudo> onde a chave  o nome da varivel XML, incluindo o caracter $ (ex.: $order)
		e conteudo  o caminho completo sobre o qual a varivel est definida.
		
		Este caminho  expresso segundo o seguinte padro: 
			nomeDocumento:nomeColecao:caminhoXpath
			Ex.: loja:informacoesLojas:Loja/Itens/Item/Pedido
		*/
		
		String path = this.forClauses.get(variableName); // definicao da variavel. Indica o documento, a colecao e o caminho xpath ao qual a variavel se refere.
		int posComma = path.indexOf(":"); // posio do primeiro caracter (:)
		String documentName = path.substring(0, posComma); // Nome do documento
		
		return documentName;		
	}
	
    public String getCollectionNameByVariableName(String variableName) throws IOException{
		
		String path = this.forClauses.get(variableName); // definicao da variavel. Indica o documento, a colecao e o caminho xpath ao qual a variavel se refere.
		int posComma = path.indexOf(":"); // posio do primeiro caracter (:)
		String subPath = path.substring(posComma+1,path.length());
		String collectionName = "";
		
		if (subPath.indexOf(":") >=0) {
			collectionName = subPath.substring(0,subPath.indexOf(":"));
		}

		return collectionName;	
		
	}

    public String getXpathByVariableName(String variableName) {
	
		String path = this.forClauses.get(variableName); // definicao da variavel. Indica o documento, a colecao e o caminho xpath ao qual a variavel se refere.		
		int posComma = path.indexOf(":");		
		String subPath = path.substring(posComma+1,path.length());				
		subPath = subPath.substring(subPath.indexOf(":")+1,subPath.length());
		
		return subPath;
	
    }
	
	public String getXpath() {
		return xpath;
	}

	public void setXpath(String xpath) {
		this.xpath = xpath;
	}

	public String getFragmentationVariable() {
		return fragmentationVariable;
	}

	public void setFragmentationVariable(String fragmentationVariable) {
		this.fragmentationVariable = fragmentationVariable;
	}

	public Hashtable<String, String> getForClauses() {
		return forClauses;
	}

	public void setForClauses(String varName, String pathName) {
		this.forClauses.put(varName,pathName);
	}
	
	public Hashtable<String, String> getSelectionPredicates() {
		return selectionPredicates;
	}

	public void setSelectionPredicates(String whereClause, String wherePath) {
		this.selectionPredicates.put(whereClause, wherePath);
	}
	
	public Hashtable<String, String> getLetClauses() {
		return letClauses;
	}

	public void setLetClauses(String varName, String referenceVarName) {
		this.letClauses.put(varName,referenceVarName);
	}

	public String getLastReadLetVariable() {
		return lastReadLetVariable;
	}

	public void setLastReadLetVariable(String lastReadLetVariable) {
		this.lastReadLetVariable = lastReadLetVariable;
	}

	public String getLastReadWhereVariable() {
		return lastReadWhereVariable;
	}

	public void setLastReadWhereVariable(String lastReadWhereVariable) {
		this.lastReadWhereVariable = lastReadWhereVariable;
	}

	public String getOperatorComparision() {
		return operatorComparision;
	}

	public void setOperatorComparision(String operatorComparision) {
		this.operatorComparision = operatorComparision;
	}

	public String getLastReadSimplePathExpr() {
		return lastReadSimplePathExpr;
	}

	public void setLastReadSimplePathExpr(String lastReadSimplePathExpr) {
		this.lastReadSimplePathExpr = lastReadSimplePathExpr;
	}

	public Hashtable<String, Hashtable<String, String>> getIndexes() {
		return indexes;
	}

	public void setIndexes(String path, Hashtable<String, String> indexIdentifier) {
		this.indexes.put(path,indexIdentifier);
	}

	public Hashtable<String, String> getIndexesToBeUsed() {
		return indexesToBeUsed;
	}

	public void setIndexesToBeUsed(String indexName, String operator) {
		this.indexesToBeUsed.put(indexName,operator);
	}

	
}

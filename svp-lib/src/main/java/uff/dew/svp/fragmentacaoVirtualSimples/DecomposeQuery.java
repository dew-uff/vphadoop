package uff.dew.svp.fragmentacaoVirtualSimples;

import java.util.ArrayList;
import java.util.Hashtable;

import uff.dew.svp.catalog.Catalog;

public class DecomposeQuery {

	protected ArrayList<String> docQueries;
	
	protected static DecomposeQuery decQry;
	private int totalNumberElements;
	private int totalNumberDocs;
	private String lastCompleteOriginalPath; // o caminho original da consulta deve ser igual ao atributo de fragmentacao 
	
	private String getLastCompleteOriginalPath() {
		return lastCompleteOriginalPath;
	}

	private void setLastCompleteOriginalPath(String lastCompleteOriginalPath) {
		this.lastCompleteOriginalPath = lastCompleteOriginalPath;
	}

	public int getTotalNumberElements() {
		return totalNumberElements;
	}

	public void setTotalNumberElements(int totalNumberElements) {
		this.totalNumberElements = totalNumberElements;
	}

	public static DecomposeQuery getUniqueInstance(boolean getunique) {		
		if (decQry == null || !getunique)
			decQry = new DecomposeQuery();
		
		return decQry;
	}
	
	public int getTotalNumberDocs() {
		return totalNumberDocs;
	}

	public void setTotalNumberDocs(int totalNumberDocs) {
		this.totalNumberDocs = totalNumberDocs;
	}
	
	public ArrayList<String> getSubQueries(String xquery, String originalQuery, int posVariable, String collectionName, String varName) {
	    		
	    String[] documentsName = Catalog.get().getDocumentsNamesForCollection(collectionName);

	    ArrayList<String> decomposedQueries = this.docQueries;    	
    	int posForClause = -1;
    	int posLetClause = -1;
    	String tmp = originalQuery;//originalQuery.substring(0, posVariable);
    	posForClause = tmp.toUpperCase().indexOf("FOR "+varName.toUpperCase()); 
    	String partXpath = "";
    	
    	String beforeClause = "";
    	String afterClause = "";
    	String subQuery = "";    	
    	
    	if ( posForClause == -1 ) { // a coleo no est especificada em uma clusula FOR, mas sim em uma clusula LET.
    		posLetClause = tmp.toUpperCase().indexOf("LET " + varName.toUpperCase());    		
    		
    		if ( posLetClause >= 0 ){
    			beforeClause = originalQuery.substring(0, posLetClause - 1);
    			afterClause = originalQuery.substring( posLetClause, originalQuery.length());    			
    		}
    	}
    	else {
    		beforeClause = originalQuery.substring(0, posForClause - 1);
			afterClause = originalQuery.substring( posForClause, originalQuery.length());
    	}   	
    	
	    if (this.docQueries==null){	    	
	    	docQueries = new ArrayList<String>();
	    }
	   	    
	    // Cria as sub-consultas substituindo nas xqueries collection(nomeColecao) pela unio dos documentos especificados em clusulas doc();	    
	    
	    if ( decomposedQueries!=null && decomposedQueries.size() > 0) {
	    	
	    	this.setTotalNumberElements(0);
	    		    	
	    	// decomposedQueries!=null: Se houver mais de um FOR/LET na consulta de entrada, obtenha as sub-consultas do FOR/LET anterior cujo collection() j foi substitudo 
	    	// por doc() e substitua os collections() do FOR/LET subsequente.
    		for ( String docqry : decomposedQueries ) {    			
    			
    			originalQuery = docqry;    	
    			String docClauses = "";
    	    	String unionClause = "( ";
    	    	String xpath = "";
    	    	String replaceTo = "";   	        	    	
    	    	
    	    	posForClause = originalQuery.toUpperCase().indexOf("FOR "+varName.toUpperCase().replace(" ", ""));    	    	
    	        beforeClause = "";
    	        afterClause = "";
    	        subQuery = "";
    	    
    	    	
    	    	if ( posForClause == -1 ) { // a coleo no est especificada em uma clusula FOR, mas sim em uma clusula LET.
    	    		posLetClause = tmp.toUpperCase().indexOf("LET " + varName.toUpperCase().replace(" ", ""));    		
    	    		
    	    		if ( posLetClause >= 0 ){
    	    			beforeClause = originalQuery.substring(0, posLetClause - 1);
    	    			afterClause = originalQuery.substring( posLetClause, originalQuery.length());
    	    		}
    	    	}
    	    	else {    	    		
    	    		beforeClause = originalQuery.substring(0, posForClause - 1);
    				afterClause = originalQuery.substring( posForClause, originalQuery.length());    				
    	    	}  	    	
    	    	
    			
    	    	posVariable = originalQuery.indexOf(varName.replace(" ", ""));
    	    	subQuery = originalQuery.substring(posVariable, originalQuery.length());
    			
    	    	String subquery = originalQuery.substring((posForClause!=-1?posForClause:posLetClause), originalQuery.length());    	    	
    	    	
			    for ( int i = 0; i < documentsName.length && subQuery.indexOf("collection(")!= -1; i++ ) {
			    	
			    	String startCollection = originalQuery.substring(originalQuery.indexOf(varName.replace(" ", "") + 
			    			(posForClause!=-1?" in collection(":" := collection(")), originalQuery.length());
			    	int posEndCollection = startCollection.indexOf(")");			    	
			    	
			    	String endCollection = startCollection.substring(posEndCollection+1, startCollection.length());			    	
			    	
			    	int posSeparator = endCollection.indexOf(" ");
			    	if (posSeparator == -1 || (endCollection.indexOf("\r\n") !=-1 && endCollection.indexOf("\r\n")<posSeparator))
			    		posSeparator = endCollection.indexOf("\r\n");
			    	if (posSeparator == -1 || (endCollection.indexOf("\n") !=-1 && endCollection.indexOf("\n")<posSeparator))
			    		posSeparator = endCollection.indexOf("\n");
			    	if (posSeparator == -1 || (endCollection.indexOf("\t")!=-1 && endCollection.indexOf("\t")<posSeparator))
			    		posSeparator = endCollection.indexOf("\t");
			    				    	
			    	xpath = endCollection.substring(0, posSeparator);			    	
			    	
			    	docClauses = docClauses 
	    						+ " let " + varName.replace(" ", "") + (i+1) + " := doc('"+documentsName[i]+"', '" +  collectionName + "')" + xpath.replace("\r\n", "")  + "\r\n";		    	
			    	
			    	unionClause = unionClause + varName.replace(" ", "") + (i+1) + (i == documentsName.length - 1? " ": " | ");
			    	
			    	partXpath = verifyCardinalityXpath(xpath, documentsName[i], collectionName, varName);
			    	
			    }			    
			    
		    	subquery = subquery.substring(0, subquery.indexOf("collection("));
		    	replaceTo = subquery + unionClause;
		    	subquery = subquery + "collection('" + collectionName + "'";
		    	
		    	String temp = afterClause.substring(afterClause.indexOf(varName.replace(" ", "")), afterClause.indexOf("collection("));
	    		
		    	
		    	afterClause = afterClause.replace(temp + "collection('" + collectionName + "')"+xpath, temp + "collection('" + collectionName + "')");
		    	
		    	String finalQuery = beforeClause + docClauses + afterClause; 
		    	finalQuery = finalQuery.replace(subquery, replaceTo);    	
		    	
		    	//newQueries.add(finalQuery);
		    	docQueries = new ArrayList<String>();
	    		docQueries.add(finalQuery);
		    	//this.docQueries.add(finalQuery);
		    	
				
    		}
    		
    		setCardinality(varName, partXpath, this.getTotalNumberElements());
    		return this.docQueries;
    	}
	    else {    	
	    	
	    	String docClauses = "";
	    	String unionClause = "( ";
	    	String xpath = ""; 
	    	
	    	setTotalNumberDocs(documentsName.length);
	    	
	    	for (int i = 0; i < documentsName.length; i++) { // enquanto houver documentos na colecao    				    	
		    	
		    	String startCollection = originalQuery.substring(originalQuery.indexOf("collection("), originalQuery.length());		    	
		    	int posEndCollection = startCollection.indexOf(")");
		    	
		    	String endCollection = startCollection.substring(posEndCollection+1, startCollection.length());	
		    	
		    	// O fim do caminho xpath at o elemento, pode ser um espao em branco, um Enter ou um Tab.
		    	int posSeparator = endCollection.indexOf(" ");
		    	if (posSeparator == -1 || (endCollection.indexOf("\r\n") !=-1 && endCollection.indexOf("\r\n")<posSeparator))
		    		posSeparator = endCollection.indexOf("\r\n");
		    	if (posSeparator == -1 || (endCollection.indexOf("\n") !=-1 && endCollection.indexOf("\n")<posSeparator))
		    		posSeparator = endCollection.indexOf("\n");
		    	if (posSeparator == -1 || (endCollection.indexOf("\t")!=-1 && endCollection.indexOf("\t")<posSeparator))
		    		posSeparator = endCollection.indexOf("\t");
		    	
		    	xpath = endCollection.substring(0, posSeparator);		    	
		    	
		    	docClauses = docClauses 
    						+ " let " + varName.replace(" ", "") + (i+1) + " := doc('"+documentsName[i]+"', '" +  collectionName + "')" + xpath + "\r\n";		    	
		    	
		    	unionClause = unionClause + varName.replace(" ", "") + (i+1) + (i == documentsName.length - 1? " ": " | "); 
		    	
		    	partXpath = verifyCardinalityXpath(xpath, documentsName[i], collectionName, varName);
		    	
		    }   
	    
		    String subquery = originalQuery.substring(posVariable, originalQuery.length());		    
	    	subquery = subquery.substring(0, subquery.indexOf("collection("));
	    	String replaceTo = subquery + unionClause;
	    	subquery = subquery + "collection('" + collectionName + "'";
	    	
	    	String temp = afterClause.substring(afterClause.indexOf(varName.replace(" ", "")), afterClause.indexOf("collection("));	    		
	    	
	    	afterClause = afterClause.replace(temp + "collection('" + collectionName + "')"+xpath, temp + "collection('" + collectionName + "')");
	    		    	
	    	String finalQuery = beforeClause + docClauses + afterClause; 
	    	finalQuery = finalQuery.replace(subquery, replaceTo);    
	    	
			this.docQueries.add(finalQuery);
			
			setCardinality(varName, partXpath, this.getTotalNumberElements());
		    	
			return this.docQueries;
	    		
	    }

	}
	
	public String verifyCardinalityXpath(String xPath, String documentName, String collectionName, String variableName) {
		
		xPath = xPath.substring(1, xPath.length()); // retirar a primeira barra do incio do caminho xpath
		
		String subPath = "";
		int posBarra = -1;
		int cardinality;
		String completePath = "";	
						
		/* Se o primeiro caracter no for barra, o usurio especificou o caminho xpath completo. (turma/estudantes/estudante/nome)
		 * Caso contrrio, especificou como //estudantes/estudante/nome		
		 * */
		if ( xPath.charAt(0) != '/' ) { 
		
			posBarra = xPath.indexOf("/"); // obtm a posio da primeira barra. Ex.: turma/estudantes/estudante/nome			
			if ( posBarra >=0 ) {	
				completePath = xPath.substring(posBarra+1, xPath.length()); // estudantes/estudante/nome		
				subPath = xPath.substring(0, posBarra); // turma				
			}
			else {
				subPath = xPath;
			}
			
			cardinality = Catalog.get().getCardinality(subPath, documentName, collectionName);
									
			if (cardinality == 1) { // No pode haver fragmentao se no houver relao 1:N; Cardinalidade do primeiro elemento
							
				do {
					posBarra = completePath.indexOf("/"); // obtm a posio da barra seguinte. Ex.: estudantes/estudante/nome			
					if ( posBarra >=0 ) {								
						subPath = subPath + "/" + completePath.substring(0, posBarra); // turma/estudantes
						completePath = completePath.substring(posBarra+1, completePath.length()); // estudante/nome
					}
					else {
						if (!completePath.equals("")) {
							subPath = subPath + "/" + completePath; // nome
							completePath = ""; 
						}
					}
					
					cardinality = Catalog.get().getCardinality(subPath, documentName, collectionName);
							
				} while (cardinality <= 1 && !completePath.equals(""));				
				
				this.setLastCompleteOriginalPath(subPath+(!completePath.equals("")?"/"+completePath:""));
				
				this.setTotalNumberElements( this.getTotalNumberElements() + cardinality);		
			}	
		}
		else { // caminho incompleto
						
			xPath = xPath.substring(1,xPath.length());
			analyzeAncestral(collectionName, documentName, variableName, xPath);
			
		}
		
		return subPath;
	}
	
	public void setCardinality(String varName, String xpath, int cardinalityNow) {
		
		Query q = Query.getUniqueInstance(true);
				
		if ( q.getLastCollectionCardinality() == 0) {
			
			if ( xpath.contains(this.getLastCompleteOriginalPath())) {
				q.setVirtualPartitioningVariable(varName);
				q.setPartitioningPath(xpath);
				q.setLastCollectionCardinality(cardinalityNow);
			}
		}
		else {
			if ( (cardinalityNow > 1 && (q.getLastCollectionCardinality() > cardinalityNow || q.getLastCollectionCardinality()<=1) )) { // se a cardinalidade da ltima unio for maior, substitui
				
				if ( xpath.contains(this.getLastCompleteOriginalPath())) {
					q.setVirtualPartitioningVariable(varName);
					q.setPartitioningPath(xpath);
					q.setLastCollectionCardinality(cardinalityNow);
				}
			}
		}

	}
	
	
public int analyzeAncestral(String collectionName, String docName, String varName, String element) {
		
		Query q = Query.getUniqueInstance(true);
		
		// GABRIEL
		// all catalog queries in this method do not use docName, although a valid value is passed
		// i am keeping it the way it was implemented, setting docName to null
		// TODO investigate this
		docName = null;
				
		this.setLastCompleteOriginalPath(element);
				
		String completePath = "";
		String completePathTmp = "";
		int cardinality = 0;
		String addedPath = "";
		int posSlash = -1;
		
		posSlash = element.indexOf("/");
		if ( posSlash >= 0) {
			completePath = element.substring(0, posSlash); // Ex.: people/person
		}
		else {
			completePath = element; // Ex.: person.
		}
		
		String parentNode = Catalog.get().getParentElement(element, collectionName, null); // does not use docname
		completePath = element;
				
		if ( parentNode!= null && !parentNode.equals("") && !parentNode.contains("Erro") ) {
			
			// enquanto a cardinalidade for zero, indica que ainda nao encontramos todos os ancestrais do elemento especificado na consulta
			while ( parentNode!= null && !parentNode.equals("") && !parentNode.contains("Erro") && cardinality == 0){
				
				completePath = parentNode + "/" + completePath;
				//completePathTmp = parentNode + "/" + completePathTmp;
				addedPath = parentNode + (!addedPath.equals("")?"/"+addedPath:addedPath);
				
				cardinality = Catalog.get().getCardinality(completePath, null, collectionName); // does not use docname
				parentNode = Catalog.get().getParentElement(parentNode, collectionName, null);
			}		
						
			String value = "";
			// Setar o caminho completo da varivel referente a coleo de dados.
			Hashtable<String, String> forClauses = (Hashtable<String, String>) q.getForClauses();
									
			if (forClauses.containsKey(varName)){
				value = forClauses.get(varName);
				value = ":"+collectionName+":"+completePath;				
				q.setForClauses(varName.trim(), value);				
				
				
			}				
			else {
				value = ":"+collectionName+":"+completePath;
				forClauses.put(varName.trim(), value);
			}
			
			// se a cardinalidade do ultimo elemento for maior que 1, verificar o caminho completo para identificar
			// o primeiro elemento com ordem N que possui pai com ordem 1.
			if ( cardinality > 1 ) {
				
				while (cardinality > 1 && !completePath.equals("")) {				
				
					posSlash = completePath.lastIndexOf("/");
					
					if ( posSlash >= 0) {
						// Ex.:    for $it in doc('xmlDataBaseXmark.xml')/site/regions/australia/item/name
						completePathTmp = completePath.substring(posSlash+1, completePath.length()) + "/" + completePathTmp; // Ex.: name
						completePath = completePath.substring(0, posSlash); // Ex.: /site/regions/australia/item
						cardinality = Catalog.get().getCardinality(completePath, null, collectionName);
					}
					else {
						if (completePathTmp.charAt(completePathTmp.length()-1) == '/') {
							completePathTmp = completePathTmp.substring(0, completePathTmp.length()-1);
						}
						cardinality = Catalog.get().getCardinality((!completePath.equals("")?completePath+"/":completePath) + completePathTmp, null, collectionName);
						completePath = ""; 
					}							
				}		
				
				String partitioningPath = (!completePath.equals("")?completePath+"/":completePath) + completePathTmp;	
				
				if (!completePath.equals("")){
				    cardinality = Catalog.get().getCardinality((!completePath.equals("")?completePath+"/":completePath) + completePathTmp, null, collectionName);
				}
				
				int posBeginning = -1;
				posBeginning = partitioningPath.indexOf("/" + addedPath + "/");
				
				if (posBeginning ==-1) {
					posBeginning = partitioningPath.indexOf(addedPath + "/");
					if (posBeginning>=0)
						partitioningPath = partitioningPath.replace(addedPath+"/", "");
				}
				else {
					partitioningPath = partitioningPath.replace("/" + addedPath+"/", "");
				}			
							
				setCardinality(varName, partitioningPath, cardinality);			
			}
			else {
				
				if (!completePathTmp.equals("") && completePathTmp.charAt(completePathTmp.length()-1) == '/') {
					completePathTmp = completePathTmp.substring(0, completePathTmp.length()-1);
					q.setAncestralPath(completePathTmp);
				}				
			}
		}
		
		return cardinality;
	}

	
}


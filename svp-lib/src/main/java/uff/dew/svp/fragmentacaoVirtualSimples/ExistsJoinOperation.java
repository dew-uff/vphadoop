package uff.dew.svp.fragmentacaoVirtualSimples;

import java.io.IOException;
import java.util.Hashtable;

import uff.dew.svp.catalog.Catalog;

public class ExistsJoinOperation {

	private boolean existsJoin;
	private String xquery;
	
	public String getXquery() {
		return xquery;
	}

	public void setXquery(String xquery) {
		this.xquery = xquery;
	}

	public boolean isExistsJoin() {
		return existsJoin;
	}

	public void setExistsJoin(boolean existsJoin) {
		this.existsJoin = existsJoin;
	}

	public ExistsJoinOperation(String inputQuery){
		this.setExistsJoin(false);
		this.setXquery(inputQuery);			
	}
	
	public void verifyInputQuery() {
		
		int pos = -1;
		String tmp = "";
		int numberForClauses = 0; // nmero de clasulas FOR		
		
		pos = this.getXquery().toUpperCase().indexOf("FOR $"); // posio da primeira clusula FOR.
		Query q = Query.getUniqueInstance(true);
		
		while ( pos >= 0 ) {		
			
			if (pos >= 0){
				numberForClauses++;
			}
			
			//pos = this.getXquery().toUpperCase().indexOf("FOR $"); // posio da primeira clusula DOC().						
			tmp = this.getXquery().substring(pos+5, this.getXquery().length()); // Obtm a string aps a definio de for.
						
			this.setXquery(tmp);
			pos = this.getXquery().toUpperCase().indexOf("FOR $"); // posio da prxima clusula FOR, se houver.*/			
			
		} 
								
		if ( numberForClauses > 1) {
			this.setExistsJoin(true);
			q.setExistsJoin(true);
		}
		else {
			this.setExistsJoin(false);
			q.setExistsJoin(false);
		}		
		
		q.setVerifiedJoins( numberForClauses - 1 );
	}	
	
	/* Recebe como parmetros os caminhos dos join */
	public void verifyJoins(String pathJoin1, String pathJoin2, String varJoin1, String varJoin2, String atrJoin1, String atrJoin2) throws IOException {		
			
		Query q = Query.getUniqueInstance(true);
		
		Hashtable<String, String> forClauses = (Hashtable<String, String>) q.getForClauses();		
					
		if (forClauses.containsKey("$"+varJoin1)){ // verifica se a varivel das junes j existem na hashtable, de forma a substituir o nome da varivel pelo caminho que ela representa.			
			pathJoin1 = pathJoin1.replace("$"+varJoin1, q.getPathVariable("$"+varJoin1));
		}
				
		if (forClauses.containsKey("$"+varJoin2)){ // verifica se a varivel das junes j existem na hashtable, de forma a substituir o nome da varivel pelo caminho que ela representa.			
			pathJoin2 = pathJoin2.replace("$"+varJoin2, q.getPathVariable("$"+varJoin2));
		}
		
		if (!q.getPathVariable("$"+varJoin1).equals("") && !q.getPathVariable("$"+varJoin2).equals("")) {
			
		String collectionName = "";
		String collectionName2 = "";
		String docName = "";
		
		int cardinality = 0;
		int cardinality2 = 0;
		if (q.getqueryExprType()!=null && q.getqueryExprType().equals("collection")) {
			collectionName = q.getCollectionNameByVariableName("$"+varJoin1);
			collectionName2 = q.getCollectionNameByVariableName("$"+varJoin2);

            // GABRIEL
            String cardStr = ExecucaoConsulta.executeQuery("let $elm := collection('" + collectionName+ "')/" + q.getPathVariable("$"+varJoin1) + " return count($elm) ");
            String cardStr2 = ExecucaoConsulta.executeQuery("let $elm := collection('" + collectionName2+ "')/" + q.getPathVariable("$"+varJoin2) + " return count($elm) ");

            cardinality = (cardStr != null && cardStr.length()>0)?Integer.parseInt(cardStr):-1;
            cardinality2 = (cardStr2 != null && cardStr2.length()>0)?Integer.parseInt(cardStr2):-1;

		}	
		else {
		
			docName = q.getDocumentNameByVariableName("$"+varJoin1);		
			cardinality = Catalog.get().getCardinality(q.getPathVariable("$"+varJoin1), docName, collectionName);
			
			String docName2 = q.getDocumentNameByVariableName("$"+varJoin2);
			cardinality2 = Catalog.get().getCardinality(q.getPathVariable("$"+varJoin2), docName2, collectionName2);
		}		
		
		if ( cardinality > 0 && cardinality2 > 0) { // especificou o caminho completo
			// Verificar se o atributo que possui a menor cardinalidade, uma vez que as chaves primrias, em geral, possuem menor 
			// cardinalidade que as chaves estrangeiras.
			
			if (cardinality < cardinality2) {
				q.setLastJoinCardinality(cardinality);
				q.setVirtualPartitioningVariable("$"+varJoin1);
				q.setPartitioningPath(pathJoin1);
				q.setLastCollectionCardinality(cardinality);
			}
			else {
				q.setLastJoinCardinality(cardinality2);
				q.setVirtualPartitioningVariable("$"+varJoin2);
				q.setPartitioningPath(pathJoin2);
				q.setLastCollectionCardinality(cardinality2);
			}								
		}
		else { // especificou caminho incompleto. Ex.: doc()//element

		    if (cardinality == 0) { // caminho da variavel do primeiro join  incompleto
				cardinality = analyzeAncestral(collectionName, docName, varJoin1, q.getPathVariable("$"+varJoin1));	
			}
			
			if (cardinality > 1) { // s pode fragmentar se houver relao 1:N.				
				
				// Obtenho sempre a que possui a menor cardinalidade, pois eh a que representa a chave primaria entre os elementos envolvidos na juncao.
				// No entanto, nas se pode executar a fragmentacao sobre um elemento com cardinalidade 1.
				if ( q.getLastJoinCardinality() == 0 
						|| (q.getLastJoinCardinality() > cardinality)
						|| q.getLastJoinCardinality() == 1) {					
					
					q.setLastJoinCardinality(cardinality);
					q.setVirtualPartitioningVariable("$"+varJoin1);					
				}
			}
			
			if (cardinality2 == 0) { // caminho da variavel do primeiro join  incompleto
							
				cardinality2 = analyzeAncestral(collectionName, docName, varJoin2, q.getPathVariable("$"+varJoin2));			
			}
			
			if (cardinality2 > 1) { // s pode fragmentar se houver relao 1:N.
								
				if ( q.getLastJoinCardinality() == 0 
						|| q.getLastJoinCardinality() > cardinality2
						|| q.getLastJoinCardinality() == 1) {
					
					q.setLastJoinCardinality(cardinality2);
					q.setVirtualPartitioningVariable("$"+varJoin2);	
				}
			}
			}
		}		
	}
	
	public int analyzeAncestral(String collectionName, String docName, String varName, String element) throws IOException {
		
		Query q = Query.getUniqueInstance(true);
		
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
		
	    String parentNode = Catalog.get().getParentElement(element, collectionName, docName);
		completePath = element;
		
		if ( parentNode!= null && !parentNode.equals("") && !parentNode.contains("Erro") ) {
			
			// enquanto a cardinalidade for zero, indica que ainda nao encontramos todos os ancestrais do elemento especificado na consulta
			while ( parentNode!= null && !parentNode.equals("") && !parentNode.contains("Erro") && cardinality == 0){
				
				completePath = parentNode + "/" + completePath;
				completePathTmp = parentNode + "/" + completePathTmp;
				addedPath = parentNode + (!addedPath.equals("")?"/"+addedPath:addedPath);
				cardinality = Catalog.get().getCardinality(completePath, docName, collectionName);
				
				parentNode = Catalog.get().getParentElement(parentNode, collectionName, docName);
			}		
			
			// se a cardinalidade do ultimo elemento for maior que 1, verificar o caminho completo para identificar
			// o primeiro elemento com ordem N que possui pai com ordem 1.
			if (cardinality > 1) {
				
				while (cardinality > 1 && !completePath.equals("")) {				
				
					posSlash = completePath.lastIndexOf("/");
					if ( posSlash >= 0) {
						// Ex.:    for $it in doc('xmlDataBaseXmark.xml')/site/regions/australia/item/name
						completePathTmp = completePath.substring(posSlash+1, completePath.length()); // Ex.: name
						completePath = completePath.substring(0, posSlash); // Ex.: /site/regions/australia/item					
					}
					else {
						completePathTmp = completePath; 
						completePath = ""; 
					}
				
					cardinality = Catalog.get().getCardinality(completePath, docName, collectionName);
				}		
				
				cardinality = Catalog.get().getCardinality((!completePath.equals("")?completePath+"/":completePath) + completePathTmp, 
				        docName, collectionName);
				q.setVirtualPartitioningVariable("$"+varName);
				q.setLastJoinCardinality(cardinality);
				
				String partitioningPath = (!completePath.equals("")?completePath+"/":completePath) + completePathTmp;
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
				
				q.setPartitioningPath(partitioningPath);
								
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

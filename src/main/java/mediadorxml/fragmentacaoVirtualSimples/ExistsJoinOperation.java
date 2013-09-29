package mediadorxml.fragmentacaoVirtualSimples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import mediadorxml.catalog.CatalogManager;

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
	
	public void verifyInputQuery() throws IOException{
		
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
	public void verifyJoins(String pathJoin1, String pathJoin2, String varJoin1, String varJoin2, String atrJoin1, String atrJoin2) throws IOException{		
			
		Query q = Query.getUniqueInstance(true);
		CatalogManager cm = CatalogManager.getUniqueInstance();
		
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
		
		String cardinality = "";
		String cardinality2 = "";
		if (q.getqueryExprType()!=null && q.getqueryExprType().equals("collection")) {
			collectionName = q.getCollectionNameByVariableName("$"+varJoin1);
			collectionName2 = q.getCollectionNameByVariableName("$"+varJoin2);
			
		    cardinality = ExecucaoConsulta.executeQuery("let $elm := collection('" + collectionName+ "')/" + q.getPathVariable("$"+varJoin1) + " return count($elm) ");
		    cardinality2 = ExecucaoConsulta.executeQuery("let $elm := collection('" + collectionName2+ "')/" + q.getPathVariable("$"+varJoin2) + " return count($elm) ");
			
		}	
		else {
		
			docName = q.getDocumentNameByVariableName("$"+varJoin1);		
			cardinality = ExecucaoConsulta.executeQuery(cm.getFormattedQuery(docName, (collectionName!=null && !collectionName.equals("")?", '"+collectionName+"'":""), q.getPathVariable("$"+varJoin1)));
			
			String docName2 = q.getDocumentNameByVariableName("$"+varJoin2);
			cardinality2 = ExecucaoConsulta.executeQuery(cm.getFormattedQuery(docName2, (collectionName2!=null && !collectionName2.equals("")?", '"+collectionName2+"'":""), q.getPathVariable("$"+varJoin2)));
			
			
		}		
		
		if ( cardinality !=null && cardinality2!=null && Integer.parseInt(cardinality) > 0 && Integer.parseInt(cardinality2) > 0) { // especificou o caminho completo
			// Verificar se o atributo que possui a menor cardinalidade, uma vez que as chaves primrias, em geral, possuem menor 
			// cardinalidade que as chaves estrangeiras.
			
			if (Integer.parseInt(cardinality)<Integer.parseInt(cardinality2)) {
				q.setLastJoinCardinality( Integer.parseInt(cardinality) );
				q.setVirtualPartitioningVariable("$"+varJoin1);
				q.setPartitioningPath(pathJoin1);
				q.setLastCollectionCardinality(Integer.parseInt(cardinality));
			}
			else {
				q.setLastJoinCardinality( Integer.parseInt(cardinality2) );
				q.setVirtualPartitioningVariable("$"+varJoin2);
				q.setPartitioningPath(pathJoin2);
				q.setLastCollectionCardinality(Integer.parseInt(cardinality2));
			}								
		}
		else { // especificou caminho incompleto. Ex.: doc()//element
			
			if ( cardinality != null && Integer.parseInt(cardinality) == 0) { // caminho da variavel do primeiro join  incompleto
				cardinality = analyzeAncestral(collectionName, docName, varJoin1, q.getPathVariable("$"+varJoin1));	
			}
			
			if ( cardinality != null && Integer.parseInt(cardinality) > 1) { // s pode fragmentar se houver relao 1:N.				
				
				// Obtenho sempre a que possui a menor cardinalidade, pois eh a que representa a chave primaria entre os elementos envolvidos na juncao.
				// No entanto, nas se pode executar a fragmentacao sobre um elemento com cardinalidade 1.
				if ( q.getLastJoinCardinality() == 0 
						|| (q.getLastJoinCardinality() > Integer.parseInt( cardinality ))
						|| q.getLastJoinCardinality() == 1) {					
					
					q.setLastJoinCardinality( Integer.parseInt(cardinality) );
					q.setVirtualPartitioningVariable("$"+varJoin1);					
				}
			}
			
			if ( cardinality2 != null && Integer.parseInt(cardinality2) == 0 ) { // caminho da variavel do primeiro join  incompleto
							
				cardinality2 = analyzeAncestral(collectionName, docName, varJoin2, q.getPathVariable("$"+varJoin2));			
			}
			
			if ( cardinality2 != null && Integer.parseInt(cardinality2) > 1) { // s pode fragmentar se houver relao 1:N.
								
				if ( q.getLastJoinCardinality() == 0 
						|| q.getLastJoinCardinality() > Integer.parseInt( cardinality2 )
						|| q.getLastJoinCardinality() == 1) {
					
					q.setLastJoinCardinality( Integer.parseInt(cardinality2));
					q.setVirtualPartitioningVariable("$"+varJoin2);	
				}
			}
			
			}
		}		
	}
	
public String analyzeAncestral(String collectionName, String docName, String varName, String element) throws IOException{
		
		CatalogManager cm = CatalogManager.getUniqueInstance();
		Query q = Query.getUniqueInstance(true);
		
		String completePath = "";
		String completePathTmp = "";
		String cardinality = "0";
		String addedPath = "";
		int posSlash = -1;
		
		posSlash = element.indexOf("/");
		if ( posSlash >= 0) {
			completePath = element.substring(0, posSlash); // Ex.: people/person
		}
		else {
			completePath = element; // Ex.: person.
		}
		
		// A fragmentao somente ser possvel se algum ancestral imediato do elemento especificado tiver cardinalidade 1.
		String xquery = " for $n in doc('$schema_" + (collectionName!=null && !collectionName.equals("")?collectionName:docName) + "')//element"
					  + " where $n/element/@name = \"" + completePath +"\""
					  + " and sum($n/@total_nodes) = 1"
					  + " return substring($n/@name,1)";
		
		ExecucaoConsulta exc = new ExecucaoConsulta();
		String parentNode = exc.executeQuery(xquery);
		completePath = element;
		
		if ( parentNode!= null && !parentNode.equals("") && !parentNode.contains("Erro") ) {
			
			// enquanto a cardinalidade for zero, indica que ainda nao encontramos todos os ancestrais do elemento especificado na consulta
			while ( parentNode!= null && !parentNode.equals("") && !parentNode.contains("Erro") && Integer.parseInt(cardinality) == 0){
				
				completePath = parentNode + "/" + completePath;
				completePathTmp = parentNode + "/" + completePathTmp;
				addedPath = parentNode + (!addedPath.equals("")?"/"+addedPath:addedPath);
				cardinality = ExecucaoConsulta.executeQuery(cm.getFormattedQuery(docName, (collectionName!=null && !collectionName.equals("")?", '"+collectionName+"'":""), completePath));
				xquery = " for $n in doc('$schema_" + (collectionName!=null && !collectionName.equals("")?collectionName:docName) + "')//element"
					   + " where $n/element/@name = \"" + parentNode +"\""
				 	   + " return substring($n/@name,1)";
				parentNode = exc.executeQuery(xquery);			
		
			}		
			
			// se a cardinalidade do ultimo elemento for maior que 1, verificar o caminho completo para identificar
			// o primeiro elemento com ordem N que possui pai com ordem 1.
			if ( Integer.parseInt(cardinality) > 1 ) {
				
				while (Integer.parseInt(cardinality) > 1 && !completePath.equals("")) {				
				
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
				
					cardinality = ExecucaoConsulta.executeQuery(cm.getFormattedQuery(docName, (collectionName!=null && !collectionName.equals("")?", '"+collectionName+"'":""), completePath));				
					
				}		
				
				cardinality = ExecucaoConsulta.executeQuery(cm.getFormattedQuery(docName, (collectionName!=null && !collectionName.equals("")?", '"+collectionName+"'":""), (!completePath.equals("")?completePath+"/":completePath) + completePathTmp));
				q.setVirtualPartitioningVariable("$"+varName);
				q.setLastJoinCardinality(Integer.parseInt(cardinality));
				
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
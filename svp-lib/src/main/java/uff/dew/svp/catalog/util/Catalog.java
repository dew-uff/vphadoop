package uff.dew.svp.catalog.util;

import java.util.ArrayList;

import uff.dew.svp.fragmentacaoVirtualSimples.Reference;

public class Catalog {
	
	protected String catalogName;	
		
	/*Carla -27/11/2010*/	
	protected ArrayList<Reference> relationships;
	protected String cardinalityQuery;
	protected String documentsQuery;
	
	// Diretrios onde os arquivo com os fragmentos e os resultados so gerados.
	// O usurio deve especificar no catlogo o caminho do arquivo no computador local, onde o sistema est sendo executado.
	protected String svpDirectory;	
	protected String avpDirectory;
	protected String partialResultDirectory;
	
	/* Parmetros utilizados para conexo com o banco de dados Sedna*/
	protected String serverName;
	protected String databaseName;
	protected String userName;
	protected String userPassword;
	
	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserPassword() {
		return userPassword;
	}

	public void setUserPassword(String userPassword) {
		this.userPassword = userPassword;
	}

	public String getPortNumber() {
		return portNumber;
	}

	public void setPortNumber(String portNumber) {
		this.portNumber = portNumber;
	}

	protected String portNumber;
	
	public String getSVP_Directory() {
		return this.svpDirectory;
	}

	public void setSVP_Directory(String sVPDirectory) {
		this.svpDirectory = sVPDirectory;
	}

	public String getAVP_Directory() {
		return this.avpDirectory;
	}

	public void setAVP_Directory(String aVPDirectory) {
		this.avpDirectory = aVPDirectory;
	}

	public String getPartialResult_Directory() {
		return this.partialResultDirectory;
	}

	public void setPartialResult_Directory(String partialResultDirectory) {
		this.partialResultDirectory = partialResultDirectory;
	}

	public String getCardinalityQuery() {		
		return this.cardinalityQuery;
	}
	
	public String getFormattedQuery(String documentName, String collectionName, String path){
		String xqueryReturn = this.cardinalityQuery;		
		xqueryReturn = xqueryReturn.replace("#", documentName); // adiciona o nome do documento
		xqueryReturn = xqueryReturn.replace("%", collectionName);  // adiciona o nome do elemento
		xqueryReturn = xqueryReturn.replace("?", path);  // adiciona o caminho at o elemento
		return xqueryReturn;
	}
	
	public String getDocumentsQuery() {		
		return this.documentsQuery;
	}
	
	public String getFormattedDocumentsQuery(String collectionName){
		String xqueryReturn = this.documentsQuery;
		xqueryReturn = xqueryReturn.replace("?", collectionName); // adiciona o nome da coleo		
		return xqueryReturn;
	}

	public void setCardinalityQuery(String xquery) {
		this.cardinalityQuery = xquery;
	}
	
	public void setDocumentsQuery(String xquery) {
		this.documentsQuery = xquery;
	}

	public Catalog(){
	    this.cardinalityQuery = "let $elm := doc('#'%)/? return count($elm)";
	    this.documentsQuery = "for $c in doc('$documents')//collection[@name='?']/document/@name return concat( substring($c,1),',')";
	}
	
	public Catalog(String catalogName){
		this.catalogName = catalogName;
	}
	
	public String getCatalogName(){
		return this.catalogName;
	}
	
	public ArrayList<Reference> getRelationships() {		
		return this.relationships;
	}

	public void setRelationships(Reference ref) {
		this.relationships = new ArrayList<Reference>();
		this.relationships.add(ref);
	}	
}

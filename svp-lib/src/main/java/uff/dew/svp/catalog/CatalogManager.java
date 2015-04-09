package uff.dew.svp.catalog;

import java.io.IOException;
import java.util.ArrayList;

import uff.dew.svp.catalog.util.Catalog;
import uff.dew.svp.fragmentacaoVirtualSimples.Reference;

public class CatalogManager {
	
	protected Catalog _catalog;
	protected static CatalogManager _catalogManager;
	
	protected static String UNION = "UNION";
	protected static String JOIN = "JOIN";
	
	public static CatalogManager getUniqueInstance() {
		if (_catalogManager == null)
			_catalogManager = new CatalogManager();
		return _catalogManager;
	}
	
	public CatalogManager() {
		this._catalog = new Catalog();
		// De-serializao do catlogo de XML para o objeto da classe "Catalog"
		//String catalogFile = Config.getCatalogFile();
//		XStream xstream = new XStream(new DomDriver());
//		this._catalog = (Catalog)xstream.fromXML(new InputStreamReader(Config.getCatalogFileInputStream()));
	}
	
	public CatalogManager(Catalog catalog){
		this._catalog = catalog;
	}
	
	public void save() throws IOException{
//		// serializao do catlogo em XML
//		String catalogFile = Config.getCatalogFile();
//		XStream xstream = new XStream(new DomDriver());
//		String xml = xstream.toXML(this._catalog);
//		try{
//			FileWriter fw = new FileWriter(catalogFile);
//			fw.write(xml);
//			fw.flush();
//			fw.close();
//			fw = null;
//		}
//		catch(IOException e){
//			// TODO Log
//			throw(e);
//		}
	}
	public ArrayList<Reference> getRelationships(){
		return this._catalog.getRelationships();
	}
	
	public String getCardinalityQuery(){
		return this._catalog.getCardinalityQuery();
	}
	
	public String getFormattedQuery(String documentName, String collectionName, String path){
		return this._catalog.getFormattedQuery(documentName, collectionName, path);
	}
	
	public String getFormattedDocumentsQuery(String collectionName){
		return this._catalog.getFormattedDocumentsQuery(collectionName);
	}
		
	public String getSVP_Directory(){
		return this._catalog.getSVP_Directory();
	}
	
	public String getAVP_Directory(){
		return this._catalog.getAVP_Directory();
	}
	
	public String getpartialResults_Directory(){
		return this._catalog.getPartialResult_Directory();
	}
	
	public String getserverName(){
		return this._catalog.getServerName();
	}
	
	public String getdatabaseName(){
		return this._catalog.getDatabaseName();
	}
	
	public String getuserName(){
		return this._catalog.getUserName();
	}
	
	public String getuserPassword(){
		return this._catalog.getUserPassword();
	}
	
	public String getportNumber(){
		return this._catalog.getPortNumber();
	}
}

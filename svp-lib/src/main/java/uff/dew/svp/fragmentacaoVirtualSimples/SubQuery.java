package uff.dew.svp.fragmentacaoVirtualSimples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.xml.xquery.XQException;
import javax.xml.xquery.XQResultSequence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.svp.SubQueryExecutionException;
import uff.dew.svp.catalog.CatalogManager;
import uff.dew.svp.db.Database;
import uff.dew.svp.db.DatabaseFactory;

public class SubQuery {
    
    private static Log LOG = LogFactory.getLog(SubQuery.class);
    
    public static final String PARTIAL_BEGIN_ELEMENT = "<partialResult>\r\n";
    public static final String PARTIAL_END_ELEMENT = "</partialResult>\r\n";
    public static final String PARTIAL_IDORDEM_BEGIN_ELEMENT = "<idOrdem>";
    public static final String PARTIAL_IDORDEM_END_ELEMENT = "</idOrdem>\r\n";

	protected static SubQuery sbq;
	protected ArrayList<String> subqueries = null;
	protected boolean sameQuery; // indica se as sub-consultas pertencem a mesma query original. Usada para as sub-consultas geradas a partir de colees.
	protected String constructorElement = ""; // define o elemento especificado pelo usurio para a estrutura da resposta. Ex.: <results><el1></el1>...</results>. Neste caso, o elemento  <results>.
	protected String elementAfterConstructor = ""; // define o elemento aps o construtor para a estrutura da resposta. Ex.: <results><order><el1></el1>...</order></results>. Neste caso, o elemento  <order>.
	protected boolean runningSubqueries;
	protected String docIdentifier = null; // usado para identificar o documento antes de armazena-lo na colecao de resultado para os casos em que as sub-consultas nao foram fragmentadas, e sao apenas consultas a documentos.
	private boolean updateOrderClause = true; // usado para identificar se h elementos em torno do elemento do order by e acertar a clusula que ser utilizada no retorno.
	
	public boolean isUpdateOrderClause() {
		return updateOrderClause;
	}

	private void setUpdateOrderClause(boolean updateOrderClause) {
		this.updateOrderClause = updateOrderClause;
	}

	public String getElementAfterConstructor() {
		return elementAfterConstructor;
	}

	public void setElementAfterConstructor(String elementAfterConstructor) {
		this.elementAfterConstructor = elementAfterConstructor;
	}

	private String getDocIdentifier() {
		
		if ( docIdentifier == null ){
			docIdentifier = "0";
		}
		
		return docIdentifier;
	}

	public void setDocIdentifier(String docIdentifier) {
		this.docIdentifier = docIdentifier;
	}

	public boolean isRunningSubqueries() {
		return runningSubqueries;
	}

	public void setRunningSubqueries(boolean runningSubqueries) {
		this.runningSubqueries = runningSubqueries;
	}

	public String getConstructorElement() {
		return constructorElement;
	}

	public void setConstructorElement(String constructorElement) {
		this.constructorElement = constructorElement;
	}
	
	public boolean isSameQuery() {
		return sameQuery;
	}

	public void setSameQuery(boolean sameQuery) {
		this.sameQuery = sameQuery;
	}

	public static SubQuery getUniqueInstance(boolean getUnique) {		
		
		if (sbq == null || !getUnique)
			sbq = new SubQuery();
		
		return sbq;
	}
	
	public void addFragment(String fragment) {
		if (this.subqueries == null)
			this.subqueries = new ArrayList<String>();
		
		this.subqueries.add(fragment);
	}
	
	public ArrayList<String> getSubQueries(){
		return this.subqueries;
	}
	
	/* Metodo para execucao das sub-consultas geradas na fragmentacao virtual simples */
	public static boolean executeSubQuery(String xquery, OutputStream out) throws SubQueryExecutionException {
	    LOG.debug("executeSubQuery() xquery: " + xquery);

	    Query q = Query.getUniqueInstance(true);

	    boolean hasResults = false;
	    
        Database db = DatabaseFactory.getSingletonDatabaseObject();
        XQResultSequence rs = null;
        try {
            // get the query inside the constructor element
            String internalQuery = getInternalQuery(xquery);
            
            // execute the internal query
            rs = db.executeQuery(internalQuery);
            
            boolean addheader = true;
            while (rs.next()) {
                String item = rs.getItemAsString(null);
            	if (addheader) {
                    hasResults = true;
                    // write XML header
                    out.write(getTitle().getBytes());
                    
                    // write partial result root element
                    out.write(PARTIAL_BEGIN_ELEMENT.getBytes());
                    
                    // write partial result constructor element
                    String header = sbq.getConstructorElement() + "\r\n";
                    out.write(header.getBytes());
                    addheader = false;
                    
                    //TODO hack
                    String element = item.substring(item.lastIndexOf("</"), item.lastIndexOf(">")+1);
                    element = element.replace("</", "<");
                    sbq.setElementAfterConstructor(element);
                }
                out.write(item.getBytes());
                out.write("\r\n".getBytes());
            }
            db.freeResources(rs);
            // if the query returned anything add the footer
            if (hasResults) {
                // close partial result constructor element
                String footer = sbq.getConstructorElement().replace("<", "</");
                out.write(footer.getBytes());
                out.write("\r\n".getBytes());

                if (!q.isOrderByClause()) { // se a consulta original nao possui order by adicione o elemento idOrdem
                    String partialOrderElement = PARTIAL_IDORDEM_BEGIN_ELEMENT + getIntervalBeginning(xquery) + PARTIAL_IDORDEM_END_ELEMENT;
                    out.write(partialOrderElement.getBytes());
                }

                // write partial result root ending element
                out.write(PARTIAL_END_ELEMENT.getBytes());
                // forces output of all data before returning
                out.flush();
            }
        }
        catch (XQException e) {
            throw new SubQueryExecutionException(e);
        }
        catch (IOException e) {
            throw new SubQueryExecutionException(e);
        }
        
        return hasResults;
	}
	
    private static String getInternalQuery(String xquery) throws IOException {

        int beginInternal = xquery.indexOf('{')+1;
        int endInternal = xquery.lastIndexOf('}');
        
        if (beginInternal == -1 || endInternal == -1) {
            throw new IOException("Query not well formed. Must have constructor element!");
        }
        
        // internalQuery doesn't have the constructor element
        String internalQuery = xquery.substring(beginInternal, endInternal);
        internalQuery.trim();
        
        LOG.debug("internal query: " + internalQuery);
        return internalQuery;
    }
	
	public static String addOrderId(String originalPartialResult, String intervalBeginning){
		
		return getTitle() + " <partialResult> \r\n" 
			              + originalPartialResult + "\r\n"
			              + " <idOrdem>" + intervalBeginning + "</idOrdem> \r\n"
			              + " </partialResult>";
		
	    
	}
	
	public static String getTitle(){
		return "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?> \r\n";
	}	
	
	public static void deleteFilesFromDirectory() throws IOException{
		
		CatalogManager cm = CatalogManager.getUniqueInstance();
		String basePath = cm.getpartialResults_Directory();
		
		File folder = new File(basePath);
	    File[] listOfFiles = folder.listFiles();

	    for ( int i = 0; i < listOfFiles.length; i++ ) {
	      
	      if ( listOfFiles[i].isFile() ) {	        
	    	  // apagar o arquivo, se existir
	    	  if (listOfFiles[i].exists()) {
	    		  listOfFiles[i].delete();
			    }
	      } 
	      
	    } // fim for

	}
	
	public static void deleteCollection() throws IOException {
		
	    try {
	        Database db = DatabaseFactory.getSingletonDatabaseObject();
	        
	        db.deleteCollection("tmpResultadosParciais");
            
	        // this is wrong, but it is expected that this method recreates the
	        // collection
	        db.createCollection("tmpResultadosParciais");
	        
        } catch (XQException e) {
            throw new IOException(e);
        }
	}
	
	public static void storeResultInXMLDocument(String partialResult, String intervalBeginning) throws IOException{
		
		CatalogManager cm = CatalogManager.getUniqueInstance();		
		String absolutePathToXMLDocuments = cm.getpartialResults_Directory();
		String fileName = "partialResult_intervalBeginning_"+ intervalBeginning + ".xml";
		String completeFileName = absolutePathToXMLDocuments + "partialResult_intervalBeginning_"+ intervalBeginning + ".xml";
		
		try {
			
		    File file = new File(completeFileName);
		    
		    if (file.exists()) {
		    	file.delete();
		    }
		    
		    FileWriter fileWriter = new FileWriter(file);
		    PrintWriter output = new PrintWriter(fileWriter);
		    output.write(partialResult);		    
		    	    
		    if ( output!=null ){
		    	output.close();
		    }
		    
		    if ( fileWriter != null ){
		    	fileWriter.close();
		    	storeXMLDocumentIntoCollection(fileName);
		    }
		    else {
		    	System.out.println("subquery.storeResultInXMLDocument: erro ao criar arquivo XML.");
		    }
		    
		} catch (IOException e) {			
			System.out.println("SubQuery.storeResultInXMLDocument: erro ao armazenar resultado parcial em documento XML.");
			e.printStackTrace();
		}		
	}
	
	/**
	 * Armazena os documentos que contm os resultados parciais na coleo temporria. 
	 * @throws IOException 
	 */
	private static void storeXMLDocumentIntoCollection(String fileName) throws IOException{
		
//		CatalogManager cm = CatalogManager.getUniqueInstance();
//		String absolutePathToXMLDocuments = cm.getpartialResults_Directory();
//		SednaConnection con = null;
//		SednaStatement st = null;		
//		
//		try {
//			
//			/* ALTERAR ESTA PARTE DO CODIGO PARA PASSAR O NUMERO DA PORTA, O SERVIDOR E O BANCO DE DADOS AO QUAL O USUARIO
//			 * DESEJA SE CONECTAR. FAZER UMA FUNCAO PARA ISSO E COLOCAR NA TELA PRINCIPAL UM TEXTFIELD PARA QUE O USUARIO
//			 * ESPECIFIQUE ESTES DADOS, PRINCIPALMENTE, O NOME DO BANCO DE DADOS. 
//			 * */			
//			con = DatabaseManager.getConnection(cm.getserverName(),cm.getdatabaseName(),cm.getuserName(),cm.getuserPassword());			
//			con.begin();
//			st = con.createStatement();
//			
//			// Verifica se a coleo j existe.
//			ExecucaoConsulta exec = new ExecucaoConsulta();
//			
//			SubQuery sbq = SubQuery.getUniqueInstance(true);			
//			
//			if (!sbq.isRunningSubqueries()) { // se nao for sub-consulta referentes a mesma query original, apagar a colecao com resultados antigos.
//			
//				if (exec.executeQuery("for $col in doc('$collections')/collections/collection/@name=\"tmpResultadosParciais\" return $col").equals("true")){				
//					// Apagar a coleo caso exista.
//					st.execute("DROP COLLECTION 'tmpResultadosParciais'");							
//				}	
//				
//				// Criar a coleo
//				st.execute("CREATE COLLECTION \"tmpResultadosParciais\"");	
//			}		
//			
//			// Armazenar documento na coleo temporria 
//			st.execute("LOAD '" + absolutePathToXMLDocuments.replace("\\", "/") + fileName + "' '" + fileName + "' 'tmpResultadosParciais'");
//			
//			con.commit();
//					
//				
//		} catch (DriverException e) {
//			System.out.println("SubQuery.storeXMLDocumentIntoCollection: Erro ao efetuar upload do documento " 
//					          + absolutePathToXMLDocuments.replace("\\", "/") + fileName + " para a coleo tmpResultadosParciais.");
//			
//			e.printStackTrace();
//		}
//		finally {
//			try {
//				if (con != null) con.close();
//				
//			} catch (DriverException e2) {
//				System.out.println("Subquery.executeSubQuery class: Erro ao fechar conexo.");
//				e2.printStackTrace();
//			}
//		}
//		
	}
	
	/***
	 * Utilizado para retornar os resultados das consultas com order by
	 * @param xqueryResult
	 * @param constructorElement
	 * @return
	 */
	public static String getElementAfterConstructorElement(String xqueryResult, String constructorElement) {
		
		String elementAfterConstructorElement = "";	
		
		constructorElement = constructorElement.replace("<", "</");
		int posConstructor = xqueryResult.indexOf(constructorElement);
				
		if ( posConstructor != -1 ) {
		
			String subpath = xqueryResult.substring(0, posConstructor);		
			int posLastEndTag = subpath.lastIndexOf("</");
			elementAfterConstructorElement = subpath.substring(posLastEndTag, subpath.lastIndexOf(">")+1);
			elementAfterConstructorElement = elementAfterConstructorElement.replace("</", "<");
		}
				
		return elementAfterConstructorElement;
	}
	
    public static String getElementAfterConstructor(String query) {
    	int returnPos = query.indexOf("return");
    	String aux = query.substring(returnPos).trim();
    	int gtPos = aux.indexOf('<');
    	int ltPos = aux.indexOf('>');
    	return aux.substring(gtPos, ltPos+1);
	}
	
	
	/***
	 * Retorna os elementos em torno dos elementos especificados no order by.
	 * Ex.: <results> {
	 *      for $order ...
	 *      order by $order/ship_date
	 *      return <order>
	 *              <date>{ $order/ship_date }</date>
	 *             </order> }
	 *      </result>
	 * A funo retornaria date.   
	 * @param xqueryResult
	 * @param constructorElement
	 * @return
	 */
	public static void getElementsAroundOrderByElement(String xquery, String elementAfterConstructor) {
		
		String elementAroundOrderBy = "";		
		String completePath = "";
		
		Query q = Query.getUniqueInstance(true);
		SubQuery sbq = SubQuery.getUniqueInstance(true);
			
		if (!q.getOrderBy().trim().equals("")) { // se a consulta original possui order by, acrescentar na consulta final o order by original.
			
			String originalClause = q.getOrderBy().trim();
			String[] orderElements = q.getOrderBy().trim().split("\\$");	
			
			String lastDeletedElement = "";
			for (int i = 0; i < orderElements.length; i++) {
				String subOrder = ""; // caminho apos a definicao da variavel. Ex.: $order/shipdate. subOrder recebe shipdate.
				int posSlash = orderElements[i].trim().indexOf("/");		
				
				subOrder = xquery.substring(xquery.indexOf(elementAfterConstructor),xquery.length()) ; // consome a string posterior ao primeiro elemento depois do construtor.
									
				if ( posSlash != -1 ) {
											
					if (orderElements[i].lastIndexOf('/') == orderElements[i].length()-1) { // se houver dois elementos na clausula order by, a string estar como $order/ship_date/$order/@id, ao separar teremos $order/ship_date/, por isso,  necessrio retirar a barra do final.
						orderElements[i] = orderElements[i].substring(0, orderElements[i].length()-1);
					}
											
					if (subOrder.trim().indexOf(elementAfterConstructor) !=-1 && subOrder.trim().indexOf(orderElements[i])!=-1) {
						subOrder = subOrder.trim().substring(subOrder.trim().indexOf(elementAfterConstructor), subOrder.trim().indexOf(orderElements[i])+1+orderElements[i].length()); // consome a parte posterior ao elemento do order by.
						
						if (subOrder.trim().indexOf(">")!=-1) {
							subOrder = subOrder.trim().substring(subOrder.trim().indexOf(">")+1, subOrder.trim().length()); // consome o elemento depois do construtor
						}							
					
						if ( !lastDeletedElement.equals("") ) {
							
							if (lastDeletedElement.indexOf("/")!=-1){ // dois elementos. Ex.: date/sp
								
								String[] last = lastDeletedElement.split("/");
								lastDeletedElement = "";
								
								for (int j = last.length-1; j >=0; j--) {
									lastDeletedElement = lastDeletedElement + "</" + last[j] + ">";									
								}
							}
							else {
								lastDeletedElement = "</" + lastDeletedElement + ">";
							}							
							
							if (subOrder.trim().indexOf(lastDeletedElement)!=-1 && subOrder.trim().indexOf(orderElements[i])!=-1) { 
								subOrder = subOrder.trim().substring(subOrder.trim().indexOf(lastDeletedElement)+lastDeletedElement.length(), subOrder.trim().indexOf(orderElements[i])+1); // consome a parte posterior ao elemento do order by.
							}
						}
						
						if ( subOrder.trim().indexOf("<")  != -1 ) { // se houver elementos entre o elemento depois do construtor e o elemento do order by, obtenha-o.
							elementAroundOrderBy = subOrder.trim().substring(subOrder.trim().indexOf("<")+1, subOrder.trim().indexOf("{")); // obtm os elementos antes do elemento do order by.Ex.:<date><sp>{$order/ship_date}</sp></date>. Retornaria date><sp>.							
							elementAroundOrderBy = elementAroundOrderBy.replaceAll("[\r\n\t' '>]", ""); // consome ENTER, TAB, espao em branco e o caracter >.
							elementAroundOrderBy = elementAroundOrderBy.replaceAll("[<]", "/"); // Substitui os caracteres que separam os elementos de < por /.

							lastDeletedElement = elementAroundOrderBy;
							completePath =  '$'+ orderElements[i].substring(0, posSlash) + "/" + (elementAroundOrderBy) + "/" + orderElements[i].substring(posSlash+1,orderElements[i].length());							
							String tmp = '$'+ orderElements[i];
							
							originalClause = originalClause.replace(tmp, completePath);
						}	
						q.setOrderBy(originalClause);
					}
					
		
				}				
			}
			
			sbq.setUpdateOrderClause(false);
		}
	}
	
	public static String getIntervalBeginning(String xquery){
		
		int posPositionFunction = xquery.indexOf("[position() ");
		String intervalBeginning = "";
		
		if ( posPositionFunction != -1 ) { // houve fragmentacao, pois ha cardinalidade 1:N entre os elementos.
			
			String subXquery = xquery.substring(posPositionFunction, xquery.length());			
			int posEqualsSymbol = subXquery.indexOf("=");
			int finalIntervalSpecification = ( subXquery.indexOf(" and") == -1? subXquery.indexOf("]"): subXquery.indexOf(" and") ); 
			intervalBeginning = subXquery.substring(posEqualsSymbol+2, finalIntervalSpecification); // soma dois para suprimir o caracter = e o espaco em branco
		}
		else { // nao houve fragmentacao
			
			SubQuery sbq = SubQuery.getUniqueInstance(true);
			int docId = Integer.parseInt(sbq.getDocIdentifier());
			docId++;
			intervalBeginning = Integer.toString(docId);
			sbq.setDocIdentifier(Integer.toString(docId));
		}		

		return intervalBeginning;
	}
	
	public static String getIntervalEnding(String xquery){
		
		int posPositionFunction = xquery.lastIndexOf("position() ");
		String intervalEnding = "";
		
		if ( posPositionFunction != -1 ) { // houve fragmentacao, pois ha cardinalidade 1:N entre os elementos.		
			String subXquery = xquery.substring(posPositionFunction, xquery.length());
			subXquery = subXquery.substring(0, subXquery.indexOf("]")+1);
			
			// se possui simbolo <, o fragmento tem tamanho maior que 1,caso contrario,  um fragmento unitrio.
			int posSymbol = ( subXquery.indexOf("<") != -1? subXquery.indexOf("<"): subXquery.indexOf("=") );			
	
			int finalIntervalSpecification = subXquery.indexOf("]");
			intervalEnding = subXquery.substring(posSymbol+2, finalIntervalSpecification);
		}		
		
		return intervalEnding;
	}
	
	public static String getConstructorElement(String xquery){
		
		String trimmedXquery = xquery.trim();
	    int posPositionLessThan = trimmedXquery.indexOf("<");	
		int posPositionGreaterThan = trimmedXquery.indexOf(">");	 
		String constructorElement = trimmedXquery.substring(posPositionLessThan, posPositionGreaterThan+1);
		// Isso ocorre quando a consulta nao retorna elementos para o intervalo especificado, retornando apenas a tag do 
		// elemento construtor. Ex.: <results/>
		if (constructorElement.indexOf("/>") != -1) {
			constructorElement = constructorElement.replace("/>", ">");
		}
	
		return constructorElement;
	}
}

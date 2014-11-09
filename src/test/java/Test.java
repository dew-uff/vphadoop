import java.io.IOException;


public class Test {

	private static String OrderBy = "$pe/name";

	public static void main(String[] args) {
		
		String query = "<results> { for $pe in doc('auction')/site/people/person let $int := $pe/profile/interest where $pe/profile/business = \"Yes\" and count($int) > 1     order by $pe/name     return     <people>         {$pe}     </people> } </results>";
		String elementAfterConstructor = "<people>";

		getElementsAroundOrderByElementOriginal(query, elementAfterConstructor);
		

	}

	public static void getElementsAroundOrderByElement(String xquery,
			String elementAfterConstructor) {

		String elementAroundOrderBy = "";
		String completePath = "";

		if (!OrderBy.trim().equals("")) { // se a consulta original possui order
											// by, acrescentar na consulta final
											// o order by original.

			String originalClause = OrderBy.trim();
			String[] orderElements = OrderBy.trim().split("\\$");

			String lastDeletedElement = "";
			for (int i = 0; i < orderElements.length; i++) {
				String subOrder = ""; // caminho apos a definicao da variavel.
										// Ex.: $order/shipdate. subOrder recebe
										// shipdate.
				int posSlash = orderElements[i].trim().indexOf("/");

				// consome a string posterior ao primeiro elemento depois do construtor.
				subOrder = xquery.substring(0, xquery.indexOf(elementAfterConstructor)+elementAfterConstructor.length()).trim(); 
				//subOrder = xquery.substring(xquery.indexOf(elementAfterConstructor),xquery.length()) ;
				
				if (posSlash != -1) {


					
					if (orderElements[i].lastIndexOf('/') == orderElements[i].length() - 1) { 
						// se houver dois elementos na clausula order by, a string $order/ship_date/$order/@id,
						// ao separar teremos $order/ship_date/, por isso, necessrio retirar a barra do final.
						orderElements[i] = orderElements[i].substring(0, orderElements[i].length() - 1);
					}

					if (subOrder.indexOf(elementAfterConstructor) != -1 && subOrder.indexOf(orderElements[i]) != -1) {
						// consome a parte posterior ao elemento do order by.
						subOrder = subOrder.substring(subOrder.indexOf(elementAfterConstructor), 
								subOrder.indexOf(orderElements[i]) + 1 + orderElements[i].length()).trim(); 
						if (subOrder.indexOf(">") != -1) {
							// consome o elemento depois do construtor
							subOrder = subOrder.substring(subOrder.indexOf(">") + 1, subOrder.length()).trim(); 						
						}

						if (!lastDeletedElement.equals("")) {
							// dois elementos. Ex.: date/sp
							if (lastDeletedElement.indexOf("/") != -1) { 

								String[] last = lastDeletedElement.split("/");
								lastDeletedElement = "";

								for (int j = last.length - 1; j >= 0; j--) {
									lastDeletedElement = lastDeletedElement
											+ "</" + last[j] + ">";
								}
							} else {
								lastDeletedElement = "</" + lastDeletedElement
										+ ">";
							}

							if (subOrder.indexOf(lastDeletedElement) != -1 && subOrder.indexOf(orderElements[i]) != -1) {
								// consome a parte posterior ao elemento do order by.
								subOrder = subOrder.substring(subOrder.indexOf(lastDeletedElement) + lastDeletedElement.length(),
										subOrder.indexOf(orderElements[i]) + 1).trim(); 
							}
						}

						// se houver elementos entre o elemento depois do construtor e o elemento
						// do order by, obtenha-o.
						if (subOrder.indexOf("<") != -1) { 
							// obtm os antes do elemento do order by.
							// Ex.:<date><sp>{$order/ship_date}</sp></date>.
							// Retornaria date><sp>.
							elementAroundOrderBy = subOrder.substring(subOrder.indexOf("<") + 1,
									subOrder.indexOf("{")).trim(); 
							// consome ENTER, TAB, espaço em branco e o caracter >.
							elementAroundOrderBy = elementAroundOrderBy.replaceAll("[\r\n\t' '>]", ""); 
							// Substitui os caracteres que separam os elementos de < por /.
							elementAroundOrderBy = elementAroundOrderBy.replaceAll("[<]", "/"); 

							lastDeletedElement = elementAroundOrderBy;
							completePath = '$'
									+ orderElements[i].substring(0, posSlash)
									+ "/"
									+ (elementAroundOrderBy)
									+ "/"
									+ orderElements[i].substring(posSlash + 1,
											orderElements[i].length());
							String tmp = '$' + orderElements[i];

							originalClause = originalClause.replace(tmp,
									completePath);
						}
						OrderBy = originalClause;
					}

				}
			}
		}

	}
	
	public static void getElementsAroundOrderByElementOriginal(String xquery, String elementAfterConstructor) {
		
		String elementAroundOrderBy = "";		
		String completePath = "";
		
		if (!OrderBy.trim().equals("")) { // se a consulta original possui order by, acrescentar na consulta final o order by original.
			
			String originalClause = OrderBy.trim();
			//System.out.println("getElementsAroundOrderByElement().inicio ="+originalClause+".");
			String[] orderElements = OrderBy.trim().split("\\$");	
			
			String lastDeletedElement = "";
			for (int i = 0; i < orderElements.length; i++) {
				String subOrder = ""; // caminho apos a definicao da variavel. Ex.: $order/shipdate. subOrder recebe shipdate.
				int posSlash = orderElements[i].trim().indexOf("/");		
				
				subOrder = xquery.substring(xquery.indexOf(elementAfterConstructor),xquery.length()) ; // consome a string posterior ao primeiro elemento depois do construtor.
									
				if ( posSlash != -1 ) {
											
					if (orderElements[i].lastIndexOf('/') == orderElements[i].length()-1) { // se houver dois elementos na clausula order by, a string estará como $order/ship_date/$order/@id, ao separar teremos $order/ship_date/, por isso, é necessário retirar a barra do final.
						orderElements[i] = orderElements[i].substring(0, orderElements[i].length()-1);
					}
					
					//System.out.println("getElementsAroundOrderByElement().orderElements[i]="+orderElements[i]+".");
					
					//System.out.println("getElementsAroundOrderByElement().subOrder -1="+subOrder+".");
					
					if (subOrder.trim().indexOf(elementAfterConstructor) !=-1 && subOrder.trim().indexOf(orderElements[i])!=-1) {
						subOrder = subOrder.trim().substring(subOrder.trim().indexOf(elementAfterConstructor), subOrder.trim().indexOf(orderElements[i])+1+orderElements[i].length()); // consome a parte posterior ao elemento do order by.
						
						//System.out.println("getElementsAroundOrderByElement().subOrder 0="+subOrder+".");
						if (subOrder.trim().indexOf(">")!=-1) {
							subOrder = subOrder.trim().substring(subOrder.trim().indexOf(">")+1, subOrder.trim().length()); // consome o elemento depois do construtor
						}
						
						//System.out.println("getElementsAroundOrderByElement().subOrder 1="+subOrder+".");
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
							//System.out.println("getElementsAroundOrderByElement().lastDeletedElement 1="+lastDeletedElement+".");
							
							if (subOrder.trim().indexOf(lastDeletedElement)!=-1 && subOrder.trim().indexOf(orderElements[i])!=-1) { 
								subOrder = subOrder.trim().substring(subOrder.trim().indexOf(lastDeletedElement)+lastDeletedElement.length(), subOrder.trim().indexOf(orderElements[i])+1); // consome a parte posterior ao elemento do order by.
							}
						}
						
						//System.out.println("getElementsAroundOrderByElement().subOrder 2="+subOrder+".");
						
						
						if ( subOrder.trim().indexOf("<")  != -1 ) { // se houver elementos entre o elemento depois do construtor e o elemento do order by, obtenha-o.
							elementAroundOrderBy = subOrder.trim().substring(subOrder.trim().indexOf("<")+1, subOrder.trim().indexOf("{")); // obtém os elementos antes do elemento do order by.Ex.:<date><sp>{$order/ship_date}</sp></date>. Retornaria date><sp>.
							//System.out.println("getElementsAroundOrderByElement().elementAroundOrderBy 1="+elementAroundOrderBy+".");
							elementAroundOrderBy = elementAroundOrderBy.replaceAll("[\r\n\t' '>]", ""); // consome ENTER, TAB, espaço em branco e o caracter >.
							//System.out.println("getElementsAroundOrderByElement().elementAroundOrderBy 2="+elementAroundOrderBy+".");
							elementAroundOrderBy = elementAroundOrderBy.replaceAll("[<]", "/"); // Substitui os caracteres que separam os elementos de < por /.
							//System.out.println("getElementsAroundOrderByElement().elementAroundOrderBy 3="+elementAroundOrderBy+".");
							lastDeletedElement = elementAroundOrderBy;
							completePath =  '$'+ orderElements[i].substring(0, posSlash) + "/" + (elementAroundOrderBy) + "/" + orderElements[i].substring(posSlash+1,orderElements[i].length());							
							String tmp = '$'+ orderElements[i];
							
							//System.out.println("getElementsAroundOrderByElement().tmp="+tmp+"."+"completepath="+completePath+",elaround:"+elementAroundOrderBy);
							originalClause = originalClause.replace(tmp, completePath);
						}	
						
						
						//System.out.println("getElementsAroundOrderByElement().originalClause="+originalClause+".");					
					
						OrderBy = originalClause;
						//q.setOrderBy(originalClause);
					}
					
		
				}				
			}
			
			//sbq.setUpdateOrderClause(false);
		}	
		
	}
}

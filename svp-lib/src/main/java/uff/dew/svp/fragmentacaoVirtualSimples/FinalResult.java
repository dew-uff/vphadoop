package uff.dew.svp.fragmentacaoVirtualSimples;

import java.io.IOException;
import java.util.Set;

public class FinalResult {

	public static String getFinalResult() throws IOException{
		
		SubQuery sbq = SubQuery.getUniqueInstance(true);
		
		String finalResultXquery = "";
		String orderByClause = "";
		String variableName = "$ret";
		
		Query q = Query.getUniqueInstance(true);
		
		
	if ( q.getAggregateFunctions() != null && q.getAggregateFunctions().size() > 0 ) { // possui funcoes de agregacao na clausula LET.
			
			finalResultXquery = sbq.getConstructorElement() + "{ \r\n"
			+	" let $c:= collection('tmpResultadosParciais')/partialResult/" + sbq.getConstructorElement().replaceAll("[</>]", "") + "\r\n"
			//+   " where $c/element()/name()!='idOrdem'" 
			+   " \r\n return \r\n\t" + "<" + sbq.getElementAfterConstructor().replaceAll("[</>]", "") + ">";

			Set<String> keys = q.getAggregateFunctions().keySet();
			for (String function : keys) {
				String expression = q.getAggregateFunctions().get(function);
					
				String elementsAroundFunction = "";
				
				if ( expression.indexOf(":") != -1 ){
					elementsAroundFunction = expression.substring( expression.indexOf(":")+1, expression.length());
					expression = expression.substring(0, expression.indexOf(":"));					
				}
				
				if ( elementsAroundFunction.indexOf("/") != -1 ) { // o elemento depois do return possui sub-elementos.
					String[] elm = elementsAroundFunction.split("/");			
					
					for (String openElement : elm) {
								
						if ( !openElement.equals("") && !openElement.equals(sbq.getElementAfterConstructor().replaceAll("[</>]", "")) )  {
							finalResultXquery = finalResultXquery + "\r\n\t\t" + " <" + openElement + ">";
						}
					}
				
					elm = elementsAroundFunction.split("/");
					String subExpression = expression.substring(expression.indexOf("$"), expression.length());
							
					if (subExpression.indexOf("/")!=-1) { // agregacao com caminho xpath. Ex.: count($c/total)
						subExpression = subExpression.substring(subExpression.indexOf("/")+1, subExpression.length());
						expression = expression.replace("$c/"+subExpression, "$c/"+elementsAroundFunction+")");
									
					}
					else { // agregacao sem caminho xpath. Ex.: count($c)
						expression = expression.replace("$c", "$c/" + elementsAroundFunction);
					}
					
					if (expression.indexOf("count(") >=0){
						expression = expression.replace("count(", "sum("); // pois deve-se somar os valores j previamente computados nos resultados parciais.
					}
					
					finalResultXquery = finalResultXquery + "{ " + expression + "}";
					
					for (int i = elm.length-1; i >= 0; i--) {
						String closeElement = elm[i];
						
						if ( !closeElement.equals("") && !closeElement.equals(sbq.getElementAfterConstructor().replaceAll("[</>]", "")) )  {
								finalResultXquery = finalResultXquery + "\r\n\t\t" + " </" + closeElement + ">";
						}						
					}					
					
				}			
				else { // apos o elemento depois do return estah a funcao de agregacao. ex.: return <resp> count($c) </resp> 
					elementsAroundFunction = "";
					expression = expression.replace("$c)", "$c/" + sbq.elementAfterConstructor.replaceAll("[</>]", "")+")");
						
					String subExpression = expression.substring(expression.indexOf("$"), expression.length());
					
					if (subExpression.indexOf("/")!=-1) { // agregacao com caminho xpath. Ex.: count($c/total)
						subExpression = subExpression.substring(subExpression.indexOf("/")+1, subExpression.length());
						expression = expression.replace("$c/"+subExpression, "$c/"+sbq.elementAfterConstructor.replaceAll("[</>]", "")+")");
							
					}
					else { // agregacao sem caminho xpath. Ex.: count($c)
						expression = expression.replace("$c", "$c/" + sbq.elementAfterConstructor.replaceAll("[</>]", ""));
					}
					
					if (expression.indexOf("count(") >=0){
						expression = expression.replace("count(", "sum("); // pois deve-se somar os valores j previamente computados nos resultados parciais.
					}
					
					finalResultXquery = finalResultXquery + "{ " + expression + "}";
				}
				
			} // fim for
			
			finalResultXquery = finalResultXquery + "\r\n\t" + sbq.elementAfterConstructor.replace("<", "</")
            + " } " + sbq.getConstructorElement().replace("<", "</");
					
		}
				
		else if (!q.getOrderBy().trim().equals("")) { // se a consulta original possui order by, acrescentar na consulta final o order by original.
			
			String[] orderElements = q.getOrderBy().trim().split("\\$");			
			for (int i = 0; i < orderElements.length; i++) {
				String subOrder = ""; // caminho apos a definicao da variavel. Ex.: $order/shipdate. subOrder recebe shipdate.
				int posSlash = orderElements[i].trim().indexOf("/");
				
				if ( posSlash != -1 ) {
					subOrder = orderElements[i].trim().substring(posSlash+1, orderElements[i].length());					
					if (subOrder.charAt(subOrder.length()-1) == '/'){
						subOrder = subOrder.substring(0, subOrder.length()-1);
					}
				}
				
				if ( !subOrder.equals("") ) {
					orderByClause = orderByClause + (orderByClause.equals("")?"": ", ") + variableName + "/key/" + subOrder;
				}
			}

//			finalResultXquery = sbq.getConstructorElement() + " { \r\n " 
//			  + " for $ret in collection('tmpResultadosParciais')/partialResult/" 
//			  + sbq.getConstructorElement().replaceAll("[</>]", "") + "/" + sbq.getElementAfterConstructor().replaceAll("[</>]", "")					          
//	          + "\r\n order by " + orderByClause + (q.getOrderByType()!=null && !q.getOrderByType().equals("")? " " + q.getOrderByType(): " ascending")
//	          + "\r\n return $ret"
//	          + "\r\n } "
//	          + sbq.getConstructorElement().replace("<", "</");  
			finalResultXquery = sbq.getConstructorElement() + " { \r\n " 
					  + " for $ret in collection('tmpResultadosParciais')/partialResult/orderby/" 
					  + sbq.getConstructorElement().replaceAll("[</>]", "") + "/" + sbq.getElementAfterConstructor().replaceAll("[</>]", "")					          
			          + "\r\n order by " + orderByClause + (q.getOrderByType()!=null && !q.getOrderByType().equals("")? " " + q.getOrderByType(): " ascending")
			          + "\r\n return $ret/element"
			          + "\r\n } "
			          + sbq.getConstructorElement().replace("<", "</");  		
		}
		else { // se a consulta original nao possui order by, acrescentar na consulta final a ordenacao de acordo com a ordem dos elementos nos documentos pesquisados.
			orderByClause = "$ret/idOrdem";
			
			finalResultXquery = sbq.getConstructorElement() + " { \r\n "
							  +	" for $ret in collection('tmpResultadosParciais')/partialResult/" + sbq.getConstructorElement().replaceAll("[</>]", "")
					          + "\r\n let $c:= $ret/element() where $ret/element()/name()!='idOrdem'"
					          + "\r\n order by " + orderByClause + " ascending"
					          + "\r\n return $c" 
					          + "\r\n } "
					          + sbq.getConstructorElement().replace("<", "</"); 

		}
		
		//System.out.println("FinalResult.getFinalResult(): QUERY COMPOSIO DOS RESULTADOS:"+finalResultXquery);
		ExecucaoConsulta exec = new ExecucaoConsulta();
		String finalResult = exec.executeQuery(finalResultXquery);
		
		return finalResult;
		
	}
}

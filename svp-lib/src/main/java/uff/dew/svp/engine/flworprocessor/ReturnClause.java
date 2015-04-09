package uff.dew.svp.engine.flworprocessor;

import java.util.ArrayList;
import java.util.Hashtable;

import uff.dew.svp.algebra.basic.TreeNode;
import uff.dew.svp.algebra.operators.AbstractOperator;
import uff.dew.svp.algebra.operators.ConstructOperator;
import uff.dew.svp.algebra.operators.SelectOperator;
import uff.dew.svp.algebra.operators.functions.FunctionOperator;
import uff.dew.svp.engine.flworprocessor.util.EnclosedExpr;
import uff.dew.svp.fragmentacaoVirtualSimples.Query;
import uff.dew.svp.javaccparser.SimpleNode;

public class ReturnClause extends Clause {
	
	protected ArrayList<AbstractOperator> _selectOperators;
	protected ArrayList<FunctionOperator> _functionOperators;
	
	private TreeNode aptNode;
	
	public ReturnClause(SimpleNode node){
		this(node, false);
	}

	public ReturnClause(SimpleNode node, boolean debug){
		
		this.operator = new ConstructOperator();
		this._selectOperators = new ArrayList<AbstractOperator>();
		this._functionOperators = new ArrayList<FunctionOperator>();
		
		this.processSimpleNode(node, debug);
		
		this.operator.getApt().setAptNode(aptNode.getRootNode());
		
	}
	
	public ArrayList<AbstractOperator> getSelectOperatorsList(){
		return this._selectOperators;
	}
	
	public ArrayList<FunctionOperator> getFunctionOperatorsList(){
		return this._functionOperators;
	}
	
	protected void processSimpleNode(SimpleNode node, boolean debug){
		if (debug)
			this.debugTrace(node);
		
		String element = node.toString();
		boolean processChild = true;	
		
		if (element.equals("ElmtConstructor")){
			String label = ((SimpleNode)node.jjtGetChild(0)).getText();
			if (aptNode != null){
				TreeNode child = new TreeNode(label, TreeNode.RelationTypeEnum.PARENT_CHILD);
				aptNode.addChild(child);
				aptNode = child;
			}
			else
				aptNode = new TreeNode(label, TreeNode.RelationTypeEnum.ROOT);
			
		}
		else if (element.equals("EnclosedExpr")){
			EnclosedExpr expr = new EnclosedExpr(node, debug);
			if (expr.getOperator().getName().equals("Select"))
				this.addSelectOperator(expr.getOperator());
			else
				this._functionOperators.add((FunctionOperator)expr.getOperator());
	
			if (aptNode != null){
				aptNode.addChild(new TreeNode(expr.getVarNodeId(),TreeNode.RelationTypeEnum.PARENT_CHILD));
			}
			else
				aptNode = new TreeNode(expr.getVarNodeId(),TreeNode.RelationTypeEnum.ROOT);
			
			processChild = false;
		}
		else if (element.equals("EndTag")){
			if ((aptNode != null) && (aptNode.getParentNode() != null))
				aptNode = aptNode.getParentNode();
		}
		else if ("QName".equals(element)){		

			Query q = Query.getUniqueInstance(true);
			String previousElement = "";
			
			if (q.getAggregateReturn()==null || (q.getAggregateReturn()!=null && q.getAggregateReturn().size()==0)) {
			
				// Armazena o ultimo elemento lido, antes do elemento atual
				previousElement = q.getElementsAroundFunction();
				//previousElement = ( !previousElement.equals("")? previousElement + "|": previousElement );
				
				if ( !q.getLastReturnVariable().equals(node.getText()) ){
				
					q.setElementsAroundFunction( previousElement + node.getText() + "|");						
					q.setLastReturnVariable(node.getText());
				}					
			}
			else {
				q.setLastReturnVariable(node.getText());
			}
			
			if (q.isWaitingXpathAggregateFunction()) {
				
				if ( q.getLastReturnVariable()!=null && q.getLastReturnVariable().equals(node.getText()) ){ // esta fechando o elemento que referencia funcao de agregacao. Ex.: <total_items>count($l)</total_items>
					
					q.setWaitingXpathAggregateFunction(false);
					// atualizar o caminho da funcao de agregacao. Se estiver como count($l), atualizar para o caminho completo count($l/order_line).
					
					Hashtable<String, String> hashTmp = q.getAggregateReturn();
					String variableName = "";
					
					if ( q.getXpathAggregateFunction() != null && q.getXpathAggregateFunction().indexOf("/") != -1 ){
						variableName = q.getXpathAggregateFunction().substring(0, q.getXpathAggregateFunction().indexOf("/"));
					}
					else if (q.getXpathAggregateFunction() != null) {
						variableName = q.getXpathAggregateFunction();
					}
					
					// usado para funcoes de agregacao presentes em predicados de selecao
					if ( variableName != null && hashTmp.get(variableName) != null && hashTmp.get(variableName).indexOf("null:") == -1) {
						
						String tmp = hashTmp.get(variableName);
						tmp = tmp.replace(variableName, q.getXpathAggregateFunction());
						q.setAggregateReturn(variableName, tmp);								
											
						q.setOrderBy(q.getOrderBy());
					}
					else { // funcoes de agregacao presentes apenas em clausulas LET e return statements
														
						String functionClause = q.getLastReadFunction() + "(" + q.getLastReadLetVariable() + ")";
						Hashtable<String, String> aggTmp = q.getAggregateFunctions();
						String tmp = aggTmp.get(functionClause);								
						tmp = tmp.replace(q.getLastReadLetVariable(), q.getXpathAggregateFunction());
													
						if ( q.getElementsAroundFunction().indexOf("|"+node.getText()+"|") != -1 ){
							String elements = q.getElementsAroundFunction().substring(0, q.getElementsAroundFunction().indexOf("|"+node.getText()+"|"));
														
							if ( !elements.equals(node.getText()) ) {
								q.setElementsAroundFunction(elements + "|");
								// acerta o caminho ate o elemento da funcao.
								elements = elements.replace("|", "/");
								tmp = tmp + ":" + elements + "/" + node.getText();
								// atualiza a variavel q.getElementsAroundFunction()
								
							}
							else {
								
								tmp = tmp + ":" + node.getText();
							}
							 
						}
						else { // ele eh o proprio elemento apos o return
							tmp = tmp + ":" + node.getText();	
						}							
						
						q.setAggregateFunc(functionClause, tmp);
						
					}
				} 
			}
			else if (q.getAggregateReturn()==null || (q.getAggregateReturn()!=null && q.getAggregateReturn().size()==0)) {
				
				if ( previousElement.indexOf("|" + node.getText() + "|") != -1 ) { // ir retirando os elementos que estao sendo fechados do caminho da funcao.
					previousElement = previousElement.substring(0, previousElement.indexOf("|" + node.getText() + "|") );
					if (previousElement.indexOf("|") != previousElement.length()-1) {
						previousElement = previousElement + "|"; // sempre deve conter | no final
					}
					q.setElementsAroundFunction(previousElement);
				}
			}
		}
		
		if (processChild & (node.jjtGetNumChildren()>0)){
			for (int i=0; i<node.jjtGetNumChildren(); i++){
				this.processSimpleNode((SimpleNode)node.jjtGetChild(i), debug);
			}
		}
	}
	
	protected void addSelectOperator(AbstractOperator operator){
		if (this._selectOperators.size()==0)
			this._selectOperators.add(operator);
		else{
			for (int i=0; i<this._selectOperators.size(); i++){
				SelectOperator sel = (SelectOperator)this._selectOperators.get(i);
				
				// Comparao dos nodos root das rvores
				if (sel.getApt().getAptRootNode().similar(operator.getApt().getAptRootNode())){
					sel.getApt().mergeTree(operator.getApt().getAptRootNode());
						return;  // conseguiu fazer o merge
				}
			}
			
			// Caso no tenha feito merge com outro Select
			this._selectOperators.add(operator);
		}
	}
}

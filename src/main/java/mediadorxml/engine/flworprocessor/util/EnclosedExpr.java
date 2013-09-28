package mediadorxml.engine.flworprocessor.util;

import java.io.IOException;

import mediadorxml.algebra.basic.TreeNode;
import mediadorxml.algebra.operators.SelectOperator;
import mediadorxml.algebra.operators.functions.FunctionAverageOperator;
import mediadorxml.algebra.operators.functions.FunctionCountOperator;
import mediadorxml.algebra.operators.functions.FunctionMaxOperator;
import mediadorxml.algebra.operators.functions.FunctionMinOperator;
import mediadorxml.algebra.operators.functions.FunctionSumOperator;
import mediadorxml.engine.flworprocessor.Clause;
import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.javaccparser.SimpleNode;

public class EnclosedExpr extends Clause {
	
	protected int _nodeId;
	protected boolean waitingVarNameFunction; // indica se esta aguardando a leitura da variavel referenciada na funcao de agregacao 

	public boolean isWaitingVarNameFunction() {
		return waitingVarNameFunction;
	}

	public void setWaitingVarNameFunction(boolean waitingVarNameFunction) {
		this.waitingVarNameFunction = waitingVarNameFunction;
	}

	public EnclosedExpr(SimpleNode node){
		this(node, false);
	}
	
	public EnclosedExpr(SimpleNode node, boolean debug){
		
		this.setOperator(new SelectOperator());
		
		this.processSimpleNode(node, debug);
	}
	
	public int getVarNodeId(){
		return this._nodeId;
	}
	
	protected void processSimpleNode(SimpleNode node, boolean debug){
		if (debug)
			this.debugTrace(node);
		
		String element = node.toString();
		boolean processChild = true;
		
		if (element.equals("VarName")){
			TreeNode newNode = new TreeNode(node.getText(), TreeNode.RelationTypeEnum.ROOT);
			this.getOperator().getApt().setAptNode(newNode);
			
			if (this.isWaitingVarNameFunction()) {
				try {
					Query q = Query.getUniqueInstance(true);		
					
					
					// Armazenar no xpath da funcao de agregacao.
					q.setXpathAggregateFunction("$"+node.getText());
					q.setWaitingXpathAggregateFunction(true);
					
					if (q.getLastReadForLetVariable().equals("")) { // funcao de agregacao apenas na clausula let, nao ha clausula For na consulta						
						q.setAggregateFunctions(q.getLastReadFunction(), "$"+node.getText(), ""); // ultimo parametro  vazio, pois a consulta deseja obter somente o resultado da funo, no h restrioes tal como count($c)>5.
					}
					else {
						String value = q.getAggregateReturn().get("$"+node.getText());
						value = value + ":" + q.getLastReturnVariable();					
						q.setAggregateFunctions(q.getLastReadFunction(), "$"+node.getText(), value); // ultimo parametro  vazio, pois a consulta deseja obter somente o resultado da funo, no h restrioes tal como count($c)>5.
					}
					this.setWaitingVarNameFunction(false);
				}
				catch (IOException e) {
					// TODO: handle exception
				}
			}
			
		}
		else if (element.equals("SimplePathExpr")){
			SimplePathExpr sp = new SimplePathExpr(node, TreeNode.MatchSpecEnum.ZERO_MORE, debug);
			if (sp.getTree() != null){
				this.getOperator().getApt().addChild(sp.getTree());
				_nodeId = sp.getVarNodeId();
			}
			else{
				_nodeId = this.getOperator().getApt().getAptNode().getNodeId();
			}
					
			processChild = false;
		}
		else if (element.equals("FuncCount")){
			this.setOperator(new FunctionCountOperator());
			this.setWaitingVarNameFunction(true);	
			
			try {
				Query q = Query.getUniqueInstance(true);
				q.setLastReadFunction("count");
			} catch (IOException e) {
				// TODO: handle exception
			}
			
		}
		else if (element.equals("FuncMax")){
			this.setOperator(new FunctionMaxOperator());
			this.setWaitingVarNameFunction(true);	
			
			try {
				Query q = Query.getUniqueInstance(true);
				q.setLastReadFunction("max");
			} catch (IOException e) {
				// TODO: handle exception
			}
		}
		else if (element.equals("FuncMin")){
			this.setOperator(new FunctionMinOperator());
			this.setWaitingVarNameFunction(true);	
			
			try {
				Query q = Query.getUniqueInstance(true);
				q.setLastReadFunction("min");
			} catch (IOException e) {
				// TODO: handle exception
			}
		}
		else if (element.equals("FuncSum")){
			this.setOperator(new FunctionSumOperator());
			
			this.setWaitingVarNameFunction(true);	
			
			try {
				Query q = Query.getUniqueInstance(true);
				q.setLastReadFunction("sum");
			} catch (IOException e) {
				// TODO: handle exception
			}
		}
		else if (element.equals("FuncAverage")){
			this.setOperator(new FunctionAverageOperator());
			
			this.setWaitingVarNameFunction(true);	
			
			try {
				Query q = Query.getUniqueInstance(true);
				q.setLastReadFunction("average");
			} catch (IOException e) {
				// TODO: handle exception
			}
		}
		
		if (processChild & (node.jjtGetNumChildren()>0)){
			for (int i=0; i<node.jjtGetNumChildren(); i++){
				this.processSimpleNode((SimpleNode)node.jjtGetChild(i), debug);
			}
		}
	}

}

package uff.dew.svp.engine.flworprocessor.util;

import java.io.IOException;

import uff.dew.svp.algebra.basic.Predicate;
import uff.dew.svp.algebra.basic.TreeNode;
import uff.dew.svp.engine.flworprocessor.Clause;
import uff.dew.svp.fragmentacaoVirtualSimples.Query;
import uff.dew.svp.javaccparser.SimpleNode;

public class ComparisonExpr extends Clause {
	
	protected String _function; 	 // Operador da funo do lado esquerdo da comparao
	protected TreeNode _node;		 // path do lado esquerdo da comparao
	protected TreeNode _nodeJoin;	 // path do lado direito da comparaao (quando for join)
	protected String _joinPredicate; // predicado da juno em relao aos LCLs

	public ComparisonExpr(SimpleNode node){
		this(node, false);
	}
	
	public ComparisonExpr(SimpleNode node, boolean debug){
		
		this.processSimpleNode(node, debug);
	}
	
	public TreeNode getTreeNode(){
		return this._node;
	}
	
	public TreeNode getTreeNodeJoin(){
		return this._nodeJoin;
	}
	
	public String getJoinPredicate(){
		return this._joinPredicate;
	}
	
	public String getFunctionName(){
		return this._function;
	}
	
	public boolean isJoinComparison(){
		return (this._nodeJoin != null);
	}
	
	public boolean isFunctionComparison(){
		return (this._function != null);
	}
	
	protected void processSimpleNode(SimpleNode node, boolean debug){
		if (debug)
			this.debugTrace(node);
		
		String element = node.toString();
		boolean processChild = true;
		
		
		if (element.equals("VarName")){		
				
			// Verificao se  a primeira varivel (se forem duas  join)
			if (this._node == null){
				// Criao de node com o nome da varivel				
				this._node = new TreeNode(node.getText(), TreeNode.RelationTypeEnum.ROOT);			
			}
			// Segunda varivel = join
			else{
				// Predicado de juno
				// Criao de node com o nome da varivel
				this._nodeJoin = new TreeNode(node.getText(), TreeNode.RelationTypeEnum.ROOT);			
				this._node.setPredicate(null);
			}		
			
		}
		
		TreeNode myNode;
		if (this.isJoinComparison())
			myNode = this._nodeJoin;
		else
			myNode = this._node;
		
		if (element.equals("SimplePathExpr")){
			SimplePathExpr sp = new SimplePathExpr(node, debug);
			int varId = 0;
			if (sp.getTree() != null){
				myNode.addChild(sp.getTree());
				varId = sp.getVarNodeId();
			}
			else
				varId = myNode.getNodeId();
			
			if (!this.isJoinComparison())
				this._node = myNode.findNode(varId);
			processChild = false;
			
			if ((this._joinPredicate == null) || (this._joinPredicate.length() == 0))
				this._joinPredicate = varId + "=";
			else
				this._joinPredicate += varId;
		}
		else if (element.equals("FunctionExpr")){
			this._function = ((SimpleNode)node.jjtGetChild(0)).toString();			
		}
		else if (element.equals("LessThan")){
			Predicate predicate = new Predicate(Predicate.ComparisonOperator.LessThan);
			myNode.setPredicate(predicate);
		}
		else if (element.equals("LessThanEquals")){
			Predicate predicate = new Predicate(Predicate.ComparisonOperator.LessThanOrEqualsTo);
			myNode.setPredicate(predicate);
		}
		else if (element.equals("GreaterThan")){
			Predicate predicate = new Predicate(Predicate.ComparisonOperator.GreaterThan);
			myNode.setPredicate(predicate);
		}
		else if (element.equals("GreaterThanEquals")){
			Predicate predicate = new Predicate(Predicate.ComparisonOperator.GreaterThanOrEqualsTo);
			myNode.setPredicate(predicate);
		}
		else if (element.equals("Equals")){
			Predicate predicate = new Predicate(Predicate.ComparisonOperator.EqualsTo);
			myNode.setPredicate(predicate);
		}
		else if (element.equals("NotEquals")){
			Predicate predicate = new Predicate(Predicate.ComparisonOperator.NotEqualsTo);
			myNode.setPredicate(predicate);
		}
		else if ((element.equals("IntegerLiteral")) || (element.equals("DecimalLiteral")) || 
				(element.equals("DoubleLiteral")) || (element.equals("StringLiteral"))){
			myNode.getPredicate().setValue(node.getText());
		}
		
		if (processChild & (node.jjtGetNumChildren()>0)){
			for (int i=0; i<node.jjtGetNumChildren(); i++){
				this.processSimpleNode((SimpleNode)node.jjtGetChild(i), debug);
			}
		}
	}

}

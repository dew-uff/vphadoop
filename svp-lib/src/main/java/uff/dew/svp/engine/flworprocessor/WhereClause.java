package uff.dew.svp.engine.flworprocessor;

import java.util.ArrayList;

import uff.dew.svp.engine.flworprocessor.util.ComparisonExpr;
import uff.dew.svp.javaccparser.SimpleNode;

public class WhereClause extends Clause {
	
	protected ArrayList<ComparisonExpr> _comparisons;	// arraylist de ComparisonExpr
	
	public WhereClause(SimpleNode node){
		this(node, false);
	}
	
	public WhereClause(SimpleNode node, boolean debug){
		this._comparisons = new ArrayList<ComparisonExpr>();
		
		this.processSimpleNode(node, debug);
	}
	
	public ArrayList<ComparisonExpr> getComparisons(){
		return this._comparisons;
	}
	
	protected void processSimpleNode(SimpleNode node, boolean debug){
		if (debug)
			this.debugTrace(node);
		
		String element = node.toString();
		boolean processChild = true;
		
		if (element == "ComparisonExpr"){
			ComparisonExpr exp = new ComparisonExpr(node, debug);
			this._comparisons.add(exp);
			processChild = false;
		}
		
		if (processChild & (node.jjtGetNumChildren()>0)){
			for (int i=0; i<node.jjtGetNumChildren(); i++){
				this.processSimpleNode((SimpleNode)node.jjtGetChild(i), debug);
			}
		}
	}
}

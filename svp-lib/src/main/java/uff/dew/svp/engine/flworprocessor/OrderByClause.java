package uff.dew.svp.engine.flworprocessor;

import java.util.ArrayList;

import uff.dew.svp.engine.flworprocessor.util.OrderSpec;
import uff.dew.svp.javaccparser.SimpleNode;

public class OrderByClause extends Clause {
	
	private ArrayList<OrderSpec> orderSpecList;

	public OrderByClause(SimpleNode node){
		this(node, false);
	}
	
	public OrderByClause(SimpleNode node, boolean debug){
		
		this.orderSpecList = new ArrayList<OrderSpec>();
		
		this.processSimpleNode(node, debug);
	}
	
	public ArrayList<OrderSpec> getOrderSpecList() {
		return orderSpecList;
	}

	protected void processSimpleNode(SimpleNode node, boolean debug){
		if (debug)
			this.debugTrace(node);
		
		String element = node.toString();
		boolean processChild = true;
		
		if (element == "OrderSpec"){
			this.orderSpecList.add(new OrderSpec(node, debug));
			processChild = false;
		}
		
		if (processChild & (node.jjtGetNumChildren()>0)){
			for (int i=0; i<node.jjtGetNumChildren(); i++){
				this.processSimpleNode((SimpleNode)node.jjtGetChild(i), debug);
			}
		}
	}
}

package mediadorxml.engine.flworprocessor;

import java.io.IOException;
import java.util.Hashtable;

import mediadorxml.algebra.basic.TreeNode;
import mediadorxml.algebra.operators.SelectOperator;
import mediadorxml.engine.flworprocessor.util.SimplePathExpr;
import mediadorxml.engine.flworprocessor.util.Variable;
import mediadorxml.fragmentacaoVirtualSimples.Query;
import mediadorxml.javaccparser.SimpleNode;

public class ForLetClause extends Clause {

	protected Variable variable;
	
	protected int times = 0; 

	protected ForLetClause(){
		super();
		this.operator = new SelectOperator();
	}
	
	public void compileForLet(SimpleNode node){
		this.compileForLet(node, false);
	}
	
	public void compileForLet(SimpleNode node, boolean debug){	
		this.processSimpleNode(node, debug);
		
		Query q;
		try {
			q = Query.getUniqueInstance(true);
			q.setFragmentationAttribute(q.getXpath());
			
			Hashtable<String, String> forClauses = (Hashtable<String, String>) q.getForClauses();			
			
			if (!forClauses.containsKey(q.getFragmentationVariable())){ // verifica se a varivel j existe na hashTable
				String variableName = q.getFragmentationVariable();
				String fragmentationAttributePath = q.getFragmentationAttribute();				
				q.setForClauses(variableName,q.getLastReadDocumentExpr()+":"+ (q.getLastReadCollectionExpr()!=null && !q.getLastReadCollectionExpr().equals("")?q.getLastReadCollectionExpr()+":":"") +fragmentationAttributePath);
			}			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Variable getVariable(){
		return this.variable;
	}
	
	protected void processSimpleNode(SimpleNode node, boolean debug){
		if (debug)
			this.debugTrace(node);
		
		String element = node.toString();
		boolean processChild = true;
		
		if ("ForClause".equals(element)){					
				
				try {
					Query q = Query.getUniqueInstance(true);
					final TreeNode newNode = new TreeNode(((SimpleNode)node.jjtGetChild(0)).getText(), TreeNode.RelationTypeEnum.ROOT);					
					q.setFragmentationVariable("$"+newNode.getLabel()); // indica a ltima varivel XML lida, referente a um FOR.					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
		}
		if ("LetClause".equals(element)){
			final TreeNode newNode = new TreeNode(((SimpleNode)node.jjtGetChild(0)).getText(), TreeNode.RelationTypeEnum.ROOT);
			try {
				Query q = Query.getUniqueInstance(true);
				q.setLastReadLetVariable("$"+newNode.getLabel()); // Varivel entre o let e o := 				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		if ("QName".equals(element)){						
			times = times+1;
			try {
				Query q = Query.getUniqueInstance(true);
				if (times>1)
					q.setLastReadCollectionExpr(node.getText());				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}
		
		if ("VarName".equals(element)){
			if (this.variable == null)
				this.variable = new Variable(node.getText());			
		}
		else if ("DocumentExpr".equals(element)){
			final TreeNode newNode = new TreeNode(((SimpleNode)node.jjtGetChild(0)).getText(), TreeNode.RelationTypeEnum.ROOT);
			this.operator.getApt().setAptNode(newNode);
			
			try {
				Query q = Query.getUniqueInstance(true);				
				
				if (!q.getLastReadDocumentExpr().equals("")) // se nome do documento j foi preenchido, o prximo texto  a definio do nome da coleo.
					q.setLastReadCollectionExpr(node.getText());
								
				// newNode.getLabel() sempre consome a extenso .xml, no entanto, se na consulta original estiver especificado a extenso
				// esta ser necessria para consultar a cardinalidade dos elementos do caminho xpath.
				
				if ( q.getInputQuery().indexOf(newNode.getLabel()+".xml") != -1 )
					q.setLastReadDocumentExpr(newNode.getLabel() + ".xml");
				else 
					q.setLastReadDocumentExpr(newNode.getLabel());
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if ("PathExpr".equals(element)){  // Referente a uma outra varivel j declarada
			String varName = ((SimpleNode)node.jjtGetChild(0)).getText();
			TreeNode newNode = new TreeNode("$" + varName, TreeNode.RelationTypeEnum.ROOT);
			this.operator.getApt().setAptNode(newNode);
		}
		else if ("SimplePathExpr".equals(element)){
			final SimplePathExpr simplePath = new SimplePathExpr(node, debug);
			this.operator.getApt().addChild(simplePath.getTree());
			this.variable.setNodeId(simplePath.getVarNodeId());
			
			// Marcao do nodo correspondente  varivel
			TreeNode aptNode = this.operator.getApt().getAptNode();
			while (aptNode.hasChield()){
				aptNode = aptNode.getChild(0);
			}
			aptNode.setIsKeyNode(true);
			
			processChild = false;
		}
		
		if (processChild & (node.jjtGetNumChildren()>0)){
			for (int i=0; i<node.jjtGetNumChildren(); i++){
				this.processSimpleNode((SimpleNode)node.jjtGetChild(i), debug);
			}
		}
	}
}

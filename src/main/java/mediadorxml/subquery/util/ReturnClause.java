package mediadorxml.subquery.util;

import mediadorxml.algebra.basic.TreeNode;

public class ReturnClause {
	
	protected TreeNode node;
	
	public ReturnClause(final TreeNode node){
		this.node = node;
	}
	
	public TreeNode getNode() {
		return node;
	}
	
	public void setNode(final TreeNode node) {
		this.node = node;
	}

	public String toString(){
		final StringBuffer ret = new StringBuffer(); 
		ret.append("return \r\n");
		
		if (node != null){
			ret.append(this.toString(node, 1));
		}
		
		return ret.toString();
	}
	
	private String toString(final TreeNode node, final int level){
		int levelInt = level;
		final StringBuffer offset = new StringBuffer();
		for (int i=0; i<levelInt; i++){
			offset.append("   ");
		}
		
		final StringBuffer ret = new StringBuffer();	
		
		if (node.getLabel().indexOf("$") >= 0){						
			
			if (node.hasChield()){
//				 Tratamento de filhos de nodos LCL (caso de $v/*)
				for (int i=0; i<node.getChildren().size(); i++){
					ret.append(offset);
					ret.append(" { ");
					ret.append(node.getLabel());
					ret.append("/" + node.getChild(i).getLabel());
					ret.append(" } \r\n");
				}
			}
			else{
				ret.append(offset);
				ret.append(" { ");
				ret.append(node.getLabel());
				ret.append(" } \r\n");
			}
		}
		else{
			if (node.hasChield()){
				levelInt++;
				ret.append(" <" + node.getLabel() + "> \r\n");
				// Priorizao dos nodos de atributos para que a query seja vlida
				for (int i=0; i<node.getChildren().size(); i++){
					if (node.getChild(i).getLabel().indexOf("@")>0){
						ret.append(this.toString(node.getChild(i), levelInt));
					}
				}
				for (int i=0; i<node.getChildren().size(); i++){
					if (node.getChild(i).getLabel().indexOf("@")==-1){
						ret.append(this.toString(node.getChild(i), levelInt));
					}
				}
				ret.append(offset);
				ret.append(" </" + node.getLabel() + "> \r\n");
			}
			else{
				ret.append(" <" + node.getLabel() + "/> \r\n");
			}
		}
		return ret.toString();
	}
}

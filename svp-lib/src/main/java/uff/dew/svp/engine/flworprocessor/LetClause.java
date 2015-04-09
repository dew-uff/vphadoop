package uff.dew.svp.engine.flworprocessor;

import uff.dew.svp.algebra.basic.TreeNode;
import uff.dew.svp.javaccparser.SimpleNode;

public class LetClause extends ForLetClause {

	public LetClause() {
		super();
		this.changeSelectAptMatch();
	}
	
	public void compileForLet(SimpleNode node){
		this.compileForLet(node, false);
		this.changeSelectAptMatch();
	}
	
	public void compileForLet(SimpleNode node, boolean debug){	
		this.processSimpleNode(node, debug);	
		this.changeSelectAptMatch();
	}
	
	private void changeSelectAptMatch(){
		if (this.getOperator().getApt().getAptRootNode() != null){
			this.changeSelectAptMatchSpec(this.getOperator().getApt().getAptRootNode());
		}
	}
	
	/**
	 * Atualizao do APT do operador Select criado para o Let para alterar
	 * todas as especificaes de cardinalidade para zero ou mais (*), o que
	 * caracteriza a diferena do operador de Select de um FOR para um LET 
	 * @param node
	 */
	private void changeSelectAptMatchSpec(final TreeNode node){
		node.setMatchSpec(TreeNode.MatchSpecEnum.ZERO_MORE);
		for (int i=0; i<node.getChildren().size(); i++){
			this.changeSelectAptMatchSpec(node.getChild(i));
		}
	}
}

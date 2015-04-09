package uff.dew.svp.algebra.basic;

import java.io.Serializable;

import uff.dew.svp.algebra.operators.AbstractOperator;
import uff.dew.svp.algebra.util.IdGenerator;

public class PatternTree implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected int aptId;
	protected TreeNode aptNode; 
	protected AbstractOperator refOperator; // Referncia do operador dono deste APT
	
	public PatternTree(){
		this.aptId = IdGenerator.getNextId();
	}
	
	public PatternTree(AbstractOperator refOperator){
		this();
		this.refOperator = refOperator;
	}
	
	public TreeNode getAptNode(){
		return this.aptNode;
	}
	
	public TreeNode getKeyNode(){
		TreeNode keyNode = null;
		if (this.aptNode != null){
			keyNode = this.aptNode.getKeyNode();
		}
		return keyNode;
	}
	
	public TreeNode getAptRootNode(){
		TreeNode rootNode = null;
		if (this.aptNode != null){
			rootNode = this.aptNode.getRootNode();
		}
		return rootNode;
	}
	
	public int getAptId(){
		return this.aptId;
	}
	
	public void setAptNode(final TreeNode node){
		this.aptNode = node;
		this.aptNode.setRefAPT(this);
	}
	
	public AbstractOperator getRefOperator() {
		return refOperator;
	}

	public void setRefOperator(final AbstractOperator operator) {
		refOperator = operator;
	}

	public boolean addChild(final TreeNode node){
		boolean returnBoolean = false;
		if ((this.aptNode == null) && (node != null)){
			this.setAptNode(node);
			this.aptNode.setRelationType(TreeNode.RelationTypeEnum.ROOT);
			this.aptNode.setIsKeyNode(true); // default  o root ser key node
			returnBoolean = true;
		}
		else if (this.aptNode != null){
			returnBoolean = this.aptNode.addChild(node);
		}
		
		return returnBoolean;
	}
		
	public void mergeTree(final TreeNode simpleTree){
		
		if ((this.aptNode.hasNode(simpleTree)) && (simpleTree.hasChield())) {
			
			for (int i=0; i<simpleTree.getChildren().size(); i++){
				this.mergeTree(simpleTree.getChild(i));
			}
			
		}
		else if (this.aptNode.hasNode(simpleTree.getParentNode())){
			final TreeNode parentSimilar = this.aptNode.findSimilarNode(simpleTree.getParentNode());
			parentSimilar.addChild(simpleTree);
		}
	}
	
	public void resetInternalIds(){
		this.aptId = IdGenerator.getNextId();
		final TreeNode node = this.aptNode.getRootNode();
		node.resetInternalIds();
	}
	
	public String toString(){
		final StringBuffer strBuffer = new StringBuffer(30);		
		strBuffer.append("//// APT id: ");
		strBuffer.append(this.aptId);
		strBuffer.append(" - refOp: "); 
		if (this.refOperator == null){
			strBuffer.append("null");
		}
		else{
			strBuffer.append(this.refOperator.getOperatorId());
		}
		
		strBuffer.append("\r\n");
		if (this.aptNode != null){
			strBuffer.append(this.aptNode.toString());
		}
		
		return strBuffer.toString();
	}

}

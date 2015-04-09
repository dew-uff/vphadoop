package uff.dew.svp.algebra.basic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.svp.algebra.util.IdGenerator;

public class TreeNode implements Cloneable, Serializable {
	
	// Tipos de RelationTypes:	
	public enum RelationTypeEnum {ROOT, PARENT_CHILD, ANCESTOR_DESCENDENT};
	// Tipos de Cardinalidades:
	public enum MatchSpecEnum {ONLYONE, ZERO_ONE, ONE_MORE, ZERO_MORE};
	//-------

	protected int nodeId;   		 		// Identificador nico do nodo (logical class id)
	protected String label; 	 		// Contedo do nodo
	protected RelationTypeEnum relationType; 		// Tipo de relacionamento com o nodo superior
	protected MatchSpecEnum matchSpec;			// Matching Specification (cardinalidade) do relacionamento com o nodo superior
	protected TreeNode parentNode;		//  Nodo pai
	protected ArrayList<TreeNode> childrenNodes;	// Nodos filhos
	protected Predicate predicate;		// Predicado (seleo)
	protected boolean isKeyNodeFlag;		// Flag que indica se o nodo  tipo chave (associado a uma varivel) da APT
	protected PatternTree refAPT;		// PatternTree (APT) dona deste TreeNode
	
	static final Log logger = LogFactory.getLog(TreeNode.class);
	
	public TreeNode(){
		this.nodeId = IdGenerator.getNextId();
		this.childrenNodes = new ArrayList<TreeNode>();
		this.matchSpec = MatchSpecEnum.ONLYONE;
		this.isKeyNodeFlag = false;
	}
	
	public TreeNode(String label, RelationTypeEnum relType){
		this();
		this.label = label;
		this.relationType = relType;
	}
	
	public TreeNode(String label){
		this();
		this.label = label;
		this.relationType = TreeNode.RelationTypeEnum.PARENT_CHILD;
	}
	
	public TreeNode(int nodeId, TreeNode.RelationTypeEnum relType){
		this("("+nodeId+")", relType);
	}
	
	public boolean addChild(final TreeNode node){
		node.setParentNode(this);
		return this.childrenNodes.add(node);
	}
	
	public boolean addChild(final ArrayList<TreeNode> nodes){
		
		boolean returnBoolean = false;
		
		if (nodes != null){
			for (int i=0; i<nodes.size(); i++){
				((TreeNode)nodes.get(i)).setParentNode(this);
			}
			returnBoolean = this.childrenNodes.addAll(nodes);
		}
		
		return returnBoolean;
	}
	
	public boolean hasChield(){
		return (this.childrenNodes == null) ? false : this.childrenNodes.size()>0;
	}
	
	public ArrayList<TreeNode> getChildren(){
		return this.childrenNodes;
	}
	
	public int getNodeId(){
		return nodeId;
	}
	
	public String getLCL(){
		return "(" + nodeId + ")";
	}
	
	public PatternTree getRefAPT() {
		return refAPT;
	}

	public void setRefAPT(final PatternTree refapt) {
		this.refAPT = refapt;
	}

	/*
	 * Ateno: o NodeId deve ser gerado pelo IdGenerator
	 */
	public void setNodeId(final int nodeId){
		this.nodeId = nodeId;
	}

	public String getLabel() {
		return label;
	}	

	public void setLabel(final String label) {
		this.label = label;
	}

	public void setLabel(final int nodeId){
		this.label = "(" + nodeId + ")";
	}
	
	public RelationTypeEnum getRelationType() {
		return relationType;
	}	

	public void setRelationType(final RelationTypeEnum type) {
		relationType = type;
	}	
	
	public MatchSpecEnum getMatchSpec() {
		return matchSpec;
	}	

	public void setMatchSpec(final MatchSpecEnum spec) {
		matchSpec = spec;
	}
	
	public TreeNode getParentNode() {
		return parentNode;
	}	

	public void setParentNode(final TreeNode node) {
		parentNode = node;
	}
	
	public Predicate getPredicate(){
		return this.predicate;
	}
	
	public void setPredicate(final Predicate predicate){
		this.predicate = predicate;
	}
	
	public ArrayList<TreeNode> getChildrenNodes() {
		return childrenNodes;
	}

	public void setChildrenNodes(final ArrayList<TreeNode> childrenNodes) {
		this.childrenNodes = childrenNodes;
	}

	public boolean isLabelLCL(){
		return (this.label.startsWith("(")) ? true : false;
	}
	
	public int getLabelLCLid(){
		int returnInt = -1;
		if (this.isLabelLCL()){
			String idString = this.label;
			idString = idString.replace('(', ' ');
			idString = idString.replace(')', ' ');
			idString = idString.trim();
			try{
				returnInt = Integer.parseInt(idString);
			}
			catch (NumberFormatException e){
				logger.error("Error parsing TreeNode label", e);
			}
		}
		return returnInt;
	}
	
	public boolean isKeyNode() {
		return isKeyNodeFlag;
	}

	public void setIsKeyNode(final boolean flag) {
		if (this.getKeyNode() != null){
			// Para garantir a unicidade do KeyNode
			this.getKeyNode().isKeyNodeFlag = false;
		}
		this.isKeyNodeFlag = flag;
	}

	public boolean hasNode(final TreeNode node){
		final TreeNode nodeFound = this.findSimilarNode(node);
		return (nodeFound == null) ? false : true;
	}
	
	public TreeNode getRootNode(){
		
		TreeNode returnNode = null;
		
		if (this.getParentNode() == null){
			returnNode = this;
		}
		else{
			TreeNode parent = this.getParentNode();
			while (parent.getParentNode() != null){
				parent = parent.getParentNode();
			}
			returnNode = parent;
		}
		return returnNode;
	}
	
	public TreeNode findNode(final int nodeId){
		
		TreeNode returnNode = null;
		
		final TreeNode rootNode = this.getRootNode();
		if (rootNode.getNodeId() == nodeId){
			returnNode = rootNode;
		}
		else{
			for (int i=0; i<this.childrenNodes.size(); i++){
				if ((this.childrenNodes.get(i)).getNodeId() == nodeId){
					returnNode = this.childrenNodes.get(i);
					break;
				}
				else{
					final TreeNode nodeFound = (this.childrenNodes.get(i)).findNode(nodeId);
					if (nodeFound != null){
						returnNode = nodeFound;
						break;
					}
				}
			}
		}
		
		return returnNode;
	}
	
	public TreeNode findNode(final String label){
		
		TreeNode returnNode = null;
		
		final TreeNode rootNode = this.getRootNode();
		if (rootNode.getLabel().equals(label)){
			returnNode = rootNode;
		}
		else{
			for (int i=0; i<this.childrenNodes.size(); i++){
				if (((TreeNode)this.childrenNodes.get(i)).getLabel().equals(label)){
					returnNode = (TreeNode)this.childrenNodes.get(i);
					break;
				}
				else{
					final TreeNode nodeFound = ((TreeNode)this.childrenNodes.get(i)).findNode(label);
					if (nodeFound != null){
						returnNode = nodeFound;
						break;
					}
				}
			}
		}
		
		return returnNode;
	}
	
	public TreeNode findSimilarNode(final TreeNode node){
		
		TreeNode returnNode = null;
		
		final TreeNode rootNode = this.getRootNode();
		if (rootNode.similar(node)){
			returnNode = rootNode;
		}
		else{
			for (int i=0; i<this.childrenNodes.size(); i++){
				if ((this.childrenNodes.get(i)).similar(node)){
					returnNode = this.childrenNodes.get(i);
					break;
				}
				else{
					final TreeNode nodeFound = (this.childrenNodes.get(i)).findSimilarNode(node);
					if (nodeFound != null){
						returnNode = nodeFound;
						break;
					}
				}
			}
		}
		
		return returnNode;
	}
	
	public TreeNode getKeyNode(){
		TreeNode rootNode = this.getRootNode();
		if (rootNode.isKeyNode()){
			return rootNode;
		}
		else{
			while (rootNode.hasChield()){
				rootNode = rootNode.getChild(0); // Um key node nunca ter irmos
				if (rootNode.isKeyNode()){
					return rootNode;
				}
			}
			return null;
		}
	}
		
	public TreeNode getChild(final int childNumber){
		if (this.childrenNodes.size() > childNumber){
			return (TreeNode)this.childrenNodes.get(childNumber);
		}
		else {
			return null;
		}
	}
	
	/**
	 * Comparao sem incluso do "id"
	 * @param object
	 * @return
	 */
	public boolean similar (final Object object){
		final TreeNode node = (TreeNode)object;
		return ((this.getLabel().equals(node.getLabel()))
			 && (this.getMatchSpec().equals(node.getMatchSpec()))
			 && (this.getRelationType().equals(node.getRelationType()))
			 && (((this.getParentNode() != null) && (this.getParentNode().similar(node.getParentNode())))
					 || ((this.getParentNode() == null) && (node.getParentNode()== null)))
			 );
	}
	
	/**
	 * Comparao do "id"
	 */
	public boolean equals (final Object object){
		final TreeNode node = (TreeNode)object;
		return (this.nodeId == node.nodeId);
	}
	
	public int hashCode(){
		//return super.hashCode();
		return this.nodeId;
	}
	
	public void resetInternalIds(){
		
		this.nodeId = IdGenerator.getNextId();
		
		if (this.getChildren() != null){
			for (int i=0; i<this.getChildren().size(); i++){
				this.getChild(i).resetInternalIds();
			}
		}
	}
	
	/*
	 *  Comparao de treeNodes para verificar se uma satisfaz as condies da outra
	 */	
	public boolean satisfies(final TreeNode onode){
		boolean satisfies = true;
		
		if (this.predicate != null){
			// Comparao do predicado do n
			TreeNode tempNode = onode;
			boolean continuar = true;
			while (continuar){
				if (tempNode.getLabel().equals(this.getLabel())){
					satisfies = this.getPredicate().satisfies(tempNode.getPredicate());
					break;
				}
				else if(tempNode.hasChield()){
					tempNode = tempNode.getChild(0);
				}
				else{
					continuar = false;
				}
			}
		}
		
		if ((satisfies) && (this.hasChield())){
			for (int i=0;i<this.getChildren().size();i++){
				satisfies = this.getChild(i).satisfies(onode);
				if (!satisfies){
					return false;  // Se algum dos nodos filhos no satisfizer, podemos retornar false
				}
			}
		}	
				
		return satisfies;
	}

	public String toString(){
		final StringBuffer buffer = new StringBuffer(55);
		// incluso do nvel na hierarquia
		if (this.parentNode != null){
			TreeNode parent = this.parentNode;
			while (parent != null){
				buffer.append('-');
				parent = parent.getParentNode();
			}
		}
		
		// Id do Nodo atual
		buffer.append(" Node Id: ");
		buffer.append(this.getNodeId());
		
		if (this.isKeyNode()){
			buffer.append(" *");
		}
		
		// Id do parent node
		buffer.append(" - Parent Id: ");
		if (this.parentNode == null){
			buffer.append("null");
		}
		else{
			buffer.append(this.parentNode.getNodeId());
			buffer.append(" type=");
			buffer.append(this.relationType);
			buffer.append(" mSpec=");
			buffer.append(this.matchSpec);
		}
		
		// Ref APT
		buffer.append(" - refAPT: ");
		if (this.refAPT == null){
			buffer.append("null");
		}
		else{
			buffer.append(this.refAPT.getAptId());
		}
		
		// Dados do nodo atual
		buffer.append(" - label: ");
		buffer.append(this.getLabel());
		
		if (this.predicate != null){
			buffer.append(' ');
			buffer.append(this.predicate.getCompOperator());
			buffer.append(' ');
			buffer.append(this.predicate.getValue());
		}
		
		buffer.append("\r\n");
		
		// Dados dos nodos filhos
		for (int i=0; i<this.childrenNodes.size();i++){
			buffer.append(((TreeNode)this.childrenNodes.get(i)).toString());
		}
		
		return buffer.toString();
	}
	
//	public TreeNode clone(){
//		// Implementao do deep clone por serializao em xml
//		final XStream xstream = new XStream(new DomDriver());
//		final String xml = xstream.toXML(this);
//		return (TreeNode)xstream.fromXML(xml);
//	}
	
	public TreeNode clone(){
		// Implementao do deep clone por serializao em memria
		TreeNode clone = null;
        try {
            // Write the object out to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            out.close();

            // Make an input stream from the byte array and read
            // a copy of the object back in.
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
            clone = (TreeNode)in.readObject();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        catch(ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
        }
        return clone;

	}
	
	
}

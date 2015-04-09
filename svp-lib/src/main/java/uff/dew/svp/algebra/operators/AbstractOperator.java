package uff.dew.svp.algebra.operators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uff.dew.svp.algebra.basic.PatternTree;
import uff.dew.svp.algebra.basic.TreeNode;
import uff.dew.svp.algebra.basic.TreeNode.MatchSpecEnum;
import uff.dew.svp.algebra.basic.TreeNode.RelationTypeEnum;
import uff.dew.svp.algebra.util.IdGenerator;
import uff.dew.svp.catalog.util.WrapperLocation;
import uff.dew.svp.exceptions.AlgebraParserException;
import uff.dew.svp.exceptions.OptimizerException;

public abstract class AbstractOperator implements Cloneable, Serializable {

	protected int operatorId;   	// Identificador nico do operador no plano
	protected String name; 		// Nome do operador
	protected PatternTree apt;  	// Annotated Pattern Tree do operador
	protected ArrayList<String> predicateList; 	// Lista de predicados do operador
	protected WrapperLocation executionSite; // Site onde o operador ser executado
	protected int estimNodesProces; // Estimativa de nodos processados pelo operador (para estimativa de custo)
	protected boolean localizationOperator; // Indicador se o operador foi gerado na etapa de localizao da query para composio dos fragmentos
	
	protected ArrayList<AbstractOperator> childOperators;
	protected AbstractOperator parentOperator;
	
	static private final Log logger = LogFactory.getLog(AbstractOperator.class);
	
	public AbstractOperator(){
		this.operatorId = IdGenerator.getNextId();
		this.apt = new PatternTree(this);
		this.childOperators = new ArrayList<AbstractOperator>();
		this.predicateList = new ArrayList<String>();
		this.localizationOperator = false;
	}
	
	public int getOperatorId(){
		return operatorId;
	}
	
	public void setOperatorId(int operatorId) {
		this.operatorId = operatorId;
	}

	public String getName(){
		return name;
	}
	
	public void setName(final String name){
		this.name = name;
	}

	public PatternTree getApt() {
		return apt;
	}
	

	public boolean isLocalizationOperator() {
		return localizationOperator;
	}

	public void setLocalizationOperator(boolean localizationOperator) {
		this.localizationOperator = localizationOperator;
	}

	public void setApt(final PatternTree apt) {
		this.apt = apt;
		this.apt.setRefOperator(this);
	}
	

	public ArrayList<String> getPredicateList() {
		return predicateList;
	}
	

	public void setPredicateList(final ArrayList<String> predicate) {
		this.predicateList = predicate;
	}	
	
	public ArrayList<AbstractOperator> getChildOperators() {
		return childOperators;
	}	

	public void setChildOperators(final ArrayList<AbstractOperator> operators) {
		childOperators = operators;
	}

	public AbstractOperator getParentOperator() {
		return parentOperator;
	}

	public void setParentOperator(final AbstractOperator operator) {
		parentOperator = operator;
	}	

	
	public int getEstimNodesProces() {
		return estimNodesProces;
	}

	public void setEstimNodesProces(final int estimNodesProces) {
		this.estimNodesProces = estimNodesProces;
	}

	public boolean addChild(final AbstractOperator operator){
		operator.setParentOperator(this);
		return this.childOperators.add(operator);
	}
	
	public boolean addChild(final AbstractOperator operator, final int position){
		boolean returnBoolean = false;
		operator.setParentOperator(this);
		try{
			this.childOperators.add(position ,operator);
			returnBoolean = true;
		}
		catch(IndexOutOfBoundsException e){
			logger.error(e);
			returnBoolean = false;
		}
		return returnBoolean;
	}
	
	public boolean hasChild(){
		return this.childOperators.size() > 0;
	}
	
	public AbstractOperator getChildAt(final int index){
		return (AbstractOperator)this.childOperators.get(index);
	}
	
	public AbstractOperator getRootOperator(){
		AbstractOperator rootOperator = null;
		if (this.parentOperator == null){
			rootOperator = this;
		}
		else{
			AbstractOperator parent = this.parentOperator;
			while(parent.getParentOperator()!= null){
				parent = parent.getParentOperator();
			}
			rootOperator = parent;
		}
		return rootOperator;
	}
	
	public WrapperLocation getExecutionSite(){
		return this.executionSite;
	}
	
	public void setExecutionSite(final WrapperLocation value){
		this.executionSite = value;
	}
	
	public void setWrapperLocation(final String view, final WrapperLocation wLocation){
		String viewName = "";
		
		if ((this.getApt() != null) && (this.getApt().getAptRootNode() != null)){
			viewName = this.getApt().getAptRootNode().getLabel();
		}
		
		if (viewName.equals(view)){
			this.setExecutionSite(wLocation);
		}
		
		if (this.hasChild()){
			for (int i=0; i<this.getChildOperators().size(); i++){
				this.getChildAt(i).setWrapperLocation(view, wLocation);
			}
		}
	}
	
	public TreeNode findNodeInPlanById(final int nodeId){
		final AbstractOperator rootOperator = this.getRootOperator();
		return rootOperator.findNodeInChildrenById(nodeId);
	}
	
	public TreeNode findNodeInChildrenById(final int nodeId){
		TreeNode returnNode = null;
		TreeNode node = this.findNodeInAptById(nodeId);
		if (node == null){
			for (int i=0; i<this.getChildOperators().size(); i++){
				node = this.getChildOperators().get(i).findNodeInChildrenById(nodeId);
				if (node != null){
					returnNode = node;
					break;
				}
			}
		}
		else{
			returnNode = node;
		}
		return returnNode;
	}
	
	public ArrayList<AbstractOperator> getOperatorsListByType(String operatorType){
		ArrayList<AbstractOperator> lista = new ArrayList<AbstractOperator>();
		final AbstractOperator rootOperator = this.getRootOperator();
		lista = rootOperator.getChildrenOperatorsListByType(operatorType);
		return lista;
	}
	
	protected ArrayList<AbstractOperator> getChildrenOperatorsListByType(String operatorType){
		final ArrayList<AbstractOperator> lista = new ArrayList<AbstractOperator>();
		
		if (this.getName().equals(operatorType)){
			lista.add(this);
		}
		
		for (int i=0; i<this.getChildOperators().size(); i++){
			final ArrayList<AbstractOperator> listaFilho = (this.getChildOperators().get(i)).getChildrenOperatorsListByType(operatorType);
			if (listaFilho.size()>0){
				lista.addAll(listaFilho);
			}
		}
		return lista;
	}
	
	public TreeNode findNodeInAptById(int nodeId){
		
		TreeNode returnNode = null;
		
		if ((this.apt != null) && (this.apt.getAptNode() != null)){
			returnNode = this.apt.getAptNode().findNode(nodeId);
		}

		return returnNode;
	}
	
	public void generateApt() throws AlgebraParserException{
		throw new AlgebraParserException("Method not Implemented in AlgebraicOperator Class");
	}
	
	public String toString(){
		StringBuffer strBuffer = new StringBuffer(150);
		strBuffer.append("////---------------------------------------\r\n");
		strBuffer.append(this.getName());
		strBuffer.append("    Id: ");
		strBuffer.append(this.getOperatorId());
		strBuffer.append("   ParentId: ");
		if (this.getParentOperator() == null){
			strBuffer.append("null");
		}
		else{
			strBuffer.append(this.getParentOperator().getOperatorId());
		}
			
		strBuffer.append("   Pred.: ");
		
		if (this.getPredicateList() != null){
			for (int i=0; i<this.getPredicateList().size(); i++){
				strBuffer.append(this.getPredicateList().get(i));
				if (i < this.getPredicateList().size()-1){
					strBuffer.append(", ");
				}
			}
		}
		
		if (this.getExecutionSite() != null){
			strBuffer.append("   Exec. Site: ");
			strBuffer.append(this.getExecutionSite().getUri());
		}
		
		strBuffer.append("\r\n");
		
		if (this.getApt() != null){
			strBuffer.append(this.getApt().toString());
		}
		
		strBuffer.append("\\\\---------------------------------------\r\n");
		
		for (int i=0; i<this.childOperators.size(); i++){
			strBuffer.append(((AbstractOperator)this.childOperators.get(i)).toString());
		}
		
		return strBuffer.toString();
	}
	
	public void resetInternalIds(){
		this.operatorId = IdGenerator.getNextId();
		this.apt.resetInternalIds();
	}
	
	/*
	 * Adiciona um operador Construct+Select sobre o operador atual
	 */
	public void addConstructOperator() throws IOException, OptimizerException{
				
		// Key Node do operador atual
		final TreeNode keyNode = this.getApt().getKeyNode();
		
		String rootNode = keyNode.getLabel();
		
		// Alterao do root node caso o operador seja um Union ou Join
		if (this.getName().equals("Union") | this.getName().equals("Join")){
			AbstractOperator op = this.getChildAt(0);
			rootNode = op.getApt().getKeyNode().getLabel();
		}
		
		// Construo da APT do Construct com root = key node
		final PatternTree aptC = new PatternTree();
		aptC.addChild(new TreeNode(rootNode, RelationTypeEnum.ROOT));
		
		// Substituiao do root node Id do Construct pelo Key Node atual
		aptC.getAptNode().setNodeId(keyNode.getNodeId());
		aptC.getAptNode().setIsKeyNode(true);
		keyNode.setNodeId(IdGenerator.getNextId());
		
//		 Construo da APT do Select com root = key node LCL
		final PatternTree aptS = new PatternTree();
		aptS.addChild(new TreeNode(keyNode.getNodeId(), RelationTypeEnum.ROOT));
		
		// Busca dos nodos utilizados pela query (rvore acima) para incluso no Construct e no Select
		final TreeNode aptNode = aptC.getAptNode(); // nodo chave para busca
		final AbstractOperator operator = this;
		
		searchAndIncludeUsedNodes(operator, aptNode, aptC, aptS);
		
		// Remoo dos nodos duplicados no Construct/Select
		TreeNode nodeConstruct = aptC.getAptRootNode();
		TreeNode nodeSelect = aptS.getAptRootNode();
		this.removeDuplicatedNodes(nodeConstruct, nodeSelect);
		
		// Construo do Construct
		ConstructOperator construct = new ConstructOperator();
		construct.setExecutionSite(this.executionSite);
		construct.setApt(aptC);
		
		// Construo do Select
		SelectOperator select = new SelectOperator();
		select.setExecutionSite(this.executionSite);
		select.setApt(aptS);
		
		// Incluso do Construct e do Select na rvore de operadores
		this.insertBelowParent(construct);
		this.insertBelowParent(select);
		
		// Alterao da especificao de cardinalidade do operador de Select para 
		// forar a sub-query ser um FOR e no um LET
		TreeNode n = this.getApt().getAptRootNode();
		if (n.hasChield()){
			do{
				n = n.getChild(0);
				n.setMatchSpec(TreeNode.MatchSpecEnum.ONLYONE);
			}
			while (!n.isKeyNode() && n.hasChield());
		}
	}
	
	private void searchAndIncludeUsedNodes(AbstractOperator operator, TreeNode k, PatternTree aptC, PatternTree aptS)
		throws IOException, OptimizerException{
	
		this.searchAndIncludeUsedNodesInAptSelect(operator, k, aptS);		
       
		// Adicionamos os nodos filhos ao Construct
		TreeNode cloneNode2 = aptS.getAptRootNode().clone();
		AbstractOperator.changeNodeLabelWithLCL(cloneNode2);
		cloneNode2.resetInternalIds();
		aptC.getAptNode().getChildren().clear();
		if (cloneNode2.hasChield()){
			aptC.getAptNode().addChild(cloneNode2.getChildren());
		}
		else{
			cloneNode2.addChild(new TreeNode("@*", RelationTypeEnum.PARENT_CHILD));
			cloneNode2.addChild(new TreeNode("*", RelationTypeEnum.PARENT_CHILD));
			aptC.getAptNode().addChild(cloneNode2);			
		}
	}

	private void searchAndIncludeUsedNodesInAptSelect(AbstractOperator operator, TreeNode k, PatternTree aptS) throws IOException, OptimizerException {
		AbstractOperator opLocal = operator;
		while (opLocal.getParentOperator() != null){
			opLocal = opLocal.getParentOperator();
			TreeNode operatorNode = opLocal.getApt().getAptRootNode();
			if (operatorNode != null){
				TreeNode kNode = operatorNode.findNode(k.getLCL());
				if (kNode != null){
					if (kNode.hasChield()){
						// adicionamos os nodos filhos ao Select
						TreeNode cloneNode = kNode.clone();
						cloneNode.resetInternalIds();						
						aptS.getAptNode().addChild(cloneNode.getChildren());
					}
					if ((!kNode.isKeyNode()) && (kNode.getKeyNode() != null)) {
						searchAndIncludeUsedNodesInAptSelect(opLocal, kNode.getKeyNode(), aptS);
					}
				}
			}
		}
		
		// Avaliao do nodo do Construct para verificao se o return no  do tipo $x
		TreeNode node = opLocal.getApt().getAptRootNode();
		if (node.hasChield() && node.getChildren().size() == 1){
			node = node.getChild(0);
			if (node.isLabelLCL()){
				TreeNode n = opLocal.findNodeInPlanById(node.getLabelLCLid());
				if (n.isLabelLCL()){
					aptS.getAptRootNode().getChildren().clear();
				}
			}
		}
	}
	
	private void removeDuplicatedNodes(TreeNode nodeConstruct, TreeNode nodeSelect){
		
		if (nodeConstruct.hasChield()){
			// Buscamos irmos duplicados e removemos um deles dos filhos do n principal
			for (int c=0; c<nodeConstruct.getChildren().size(); c++){
				TreeNode nc = nodeConstruct.getChild(c);
				String label = null;
				if (nc.isLabelLCL()){
					int lcl = nc.getLabelLCLid();
					label = nodeSelect.findNode(lcl).getLabel();					
				}
				else{
					label = nc.getLabel();
				}
				//Buscaremos nos outros irmos algum dublicado
				for (int i=c+1; i<nodeConstruct.getChildren().size(); i++){
					TreeNode ni = nodeConstruct.getChild(i);
					String labelIrmao = null;
					if (ni.isLabelLCL()){
						int lcl = ni.getLabelLCLid();
						labelIrmao = nodeSelect.findNode(lcl).getLabel();					
					}
					else{
						labelIrmao = ni.getLabel();
					}
					
					// Comparao dos labels
					if (label.equals(labelIrmao)){
						// Se for igual, removemos o irmo
						nc.addChild(ni.getChildren());
						
						TreeNode p = ni.getParentNode();
						p.getChildren().remove(ni);
						i--;
						c--;
						
						// Remoo do nodo equivalente do Select
						int lclC = nc.getLabelLCLid();
						int lclI = ni.getLabelLCLid();
						TreeNode nsC = nodeSelect.findNode(lclC);
						TreeNode nsI = nodeSelect.findNode(lclI);
						nsC.addChild(nsI.getChildren());
						TreeNode nsIp = nsI.getParentNode();
						nsIp.getChildren().remove(nsI);						
					}
				}
				
				this.removeDuplicatedNodes(nc, nodeSelect);
			}
		}
	}
	
	public void insertBelowParent(AbstractOperator op){
		AbstractOperator parent = this.getParentOperator();
		if (parent == null){
			op.addChild(this);
		}
		else{			
			int position = parent.getChildOperators().indexOf(this);
			parent.getChildOperators().remove(this);
			parent.addChild(op, position);
			op.addChild(this);
		}
	}
	
	protected static void changeNodeLabelWithLCL(TreeNode node){
		
		// Nodos com filhos no podem ser LCL
		if (!node.hasChield()){
			node.setLabel(node.getNodeId());
		}
		
		node.setMatchSpec(MatchSpecEnum.ONLYONE);
		if (node.hasChield()){
			for (int i=0; i<node.getChildren().size(); i++){
				TreeNode child = node.getChild(i);
				AbstractOperator.changeNodeLabelWithLCL(child);
			}
		}
	}
	
	public AbstractOperator clone() throws CloneNotSupportedException{
		// Implementao do deep clone por serializao em memria
		AbstractOperator clone = null;
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
            clone = (AbstractOperator)in.readObject();
        }
        catch(IOException e) {
            logger.error(e);
        }
        catch(ClassNotFoundException cnfe) {
        	logger.error(cnfe);
        }
        return clone;
	}
}

package uff.dew.svp.catalog.util;

public class ProjectionPredicate {
	
	protected String rootNode;
	protected String[] excludedNodes;
	protected String[] keyNodes;

	public ProjectionPredicate(){
	}

	public String[] getExcludedNodes() {
		return excludedNodes;
	}

	public void setExcludedNodes(String[] excludedNodes) {
		this.excludedNodes = excludedNodes;
	}

	public String getRootNode() {
		return rootNode;
	}

	public void setRootNode(String rootNode) {
		this.rootNode = rootNode;
	}

	public String[] getKeyNodes() {
		return keyNodes;
	}

	public void setKeyNodes(String[] keyNodes) {
		this.keyNodes = keyNodes;
	}
	
	
}

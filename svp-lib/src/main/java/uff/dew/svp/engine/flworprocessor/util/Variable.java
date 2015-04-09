package uff.dew.svp.engine.flworprocessor.util;

public class Variable {
	
	protected String _varName;
	protected int _nodeId;

	public Variable(String varName){
		this._varName = varName;
	}
	
	public Variable(String varName, int nodeId){
		this(varName);
		this._nodeId = nodeId;
	}
	
	public String getVarName(){
		return this._varName;
	}
	
	public int getNodeId(){
		return this._nodeId;
	}
	
	public void setNodeId(int nodeId){
		this._nodeId = nodeId;
	}
}

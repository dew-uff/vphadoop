package mediadorxml.subquery.util;

import java.util.ArrayList;

public abstract class ForLetClause {
	
	protected String _variable;
	protected ArrayList<String> _sources;
	
	public ForLetClause(String varName){
		this._variable = varName;
		this._sources = new ArrayList<String>();
	}

	public ArrayList getSources() {
		return _sources;
	}

	public void setSources(ArrayList<String> sources) {
		this._sources = sources;
	}
	
	public void addSource(String source){
		this._sources.add(source);
	}

	public String getVariable() {
		return _variable;
	}

	public void setVariable(String variable) {
		this._variable = variable;
	}
	
	public String toString(){
		String ret = "";
		if ((this._sources != null) && (this._sources.size() > 0)){
			if (this._sources.size() > 1){
				ret += "(";
				for (int i=0; i<this._sources.size(); i++){
					ret += this._sources.get(i);
					if (i < this._sources.size()-1){
						ret += " | ";
					}
				}
				ret += ")";
			}
			else{
				ret += (String)this._sources.get(0);
			}
		}
		ret += "\r\n";
		return ret;
	}
}

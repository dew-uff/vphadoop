package mediadorxml.subquery.util;

import java.util.ArrayList;

public class WhereClause {
	
	protected ArrayList<String> _ands;
	
	public WhereClause(){
		this._ands = new ArrayList<String>();
	}
	
	public ArrayList<String> getAnds() {
		return _ands;
	}

	public void setAnds(ArrayList<String> ands) {
		this._ands = ands;
	}

	public void addAnd(String and){
		if (!this._ands.contains(and)){
			this._ands.add(and);
		}
	}

	public String toString(){
		String ret = "";
		if (this._ands.size() > 0){
			ret = "where ";
			for (int i=0; i<this._ands.size(); i++){
				ret += this._ands.get(i);
				if (i < this._ands.size()-1){
					ret += " and ";
				}
			}
			ret += "\r\n";
		}
		return ret;
	}

}

package mediadorxml.subquery.util;

public class ForClause extends ForLetClause {

	public ForClause(String varName){
		super(varName);
	}
	
	public String toString(){
		String ret = "for " + this._variable + " in ";
		ret += super.toString();		
		return ret;
	}
	
}

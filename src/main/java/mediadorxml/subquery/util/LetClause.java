package mediadorxml.subquery.util;

public class LetClause extends ForLetClause {
		
	public LetClause(String varName){
		super(varName);
	}

	public String toString(){
		String ret = "let " + this._variable + " := ";
		ret += super.toString();		
		return ret;
	}
	
}

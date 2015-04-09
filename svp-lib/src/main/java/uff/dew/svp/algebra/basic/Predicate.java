package uff.dew.svp.algebra.basic;

import java.io.Serializable;

public class Predicate implements Serializable {
	
	public enum ComparisonOperator {LessThan, GreaterThan, EqualsTo, NotEqualsTo, LessThanOrEqualsTo, GreaterThanOrEqualsTo};
	
	protected String _value;
	protected ComparisonOperator _comp;
	
	public Predicate(){
	}
	
	public Predicate(ComparisonOperator op){
		this._comp = op;
	}
	
	public Predicate(ComparisonOperator op, String value){
		this(op);
		this._value = value;
	}

	public ComparisonOperator getCompOperator(){
		return this._comp;
	}
	
	public void setCompOperator(ComparisonOperator op){
		this._comp = op;
	}
	
	public String getValue(){
		return this._value;
	}
	
	public void setValue(String value){
		this._value = value;
	}
	
	public boolean satisfies(Predicate opred){
		boolean satisfies = true;
		
		if ((this.getCompOperator().equals(ComparisonOperator.EqualsTo))
				&& (opred.getCompOperator().equals(ComparisonOperator.EqualsTo))){
			if (this.getValue().trim().equals(opred.getValue().trim()))
				return true;
			else
				return false; 
		}
		
		double valMe, valPred;
		try{
			valMe = Double.parseDouble(this.getValue().trim());
			valPred = Double.parseDouble(opred.getValue().trim());
		}
		catch (Exception e){
			// TODO Log
			return false;
		}
		
		switch (this.getCompOperator()){
			case GreaterThan:
				switch (opred.getCompOperator()){
					case GreaterThan:
					case GreaterThanOrEqualsTo:
						return true;
					case LessThan:
					case LessThanOrEqualsTo:
					case EqualsTo:
						if (valPred > valMe)
							return true;
						else
							return false;
				}
			case GreaterThanOrEqualsTo:
				switch (opred.getCompOperator()){
					case GreaterThan:
					case GreaterThanOrEqualsTo:
						return true;
					case LessThan:
						if (valPred > valMe)
							return true;
						else
							return false;
					case LessThanOrEqualsTo:
					case EqualsTo:
						if (valPred >= valMe)
							return true;
						else
							return false;						
				}
				break;
			case LessThan:
				switch (opred.getCompOperator()){
					case GreaterThan:
					case GreaterThanOrEqualsTo:
					case EqualsTo:
						if (valPred < valMe)
							return true;
						else
							return false;
					case LessThan:
					case LessThanOrEqualsTo:
						return true;
				}
				break;
			case LessThanOrEqualsTo:
				switch (opred.getCompOperator()){
					case GreaterThan:
						if (valPred < valMe)
							return true;
						else
							return false;
					case GreaterThanOrEqualsTo:
					case EqualsTo:
						if (valPred <= valMe)
							return true;
						else
							return false;
					case LessThan:
					case LessThanOrEqualsTo:
						return true;
				}
				break;
			case EqualsTo:
				switch (opred.getCompOperator()){
					case GreaterThan:
						if (valPred < valMe)
							return true;
						else
							return false;
					case GreaterThanOrEqualsTo:
						if (valPred <= valMe)
							return true;
						else
							return false;						
					case EqualsTo:
						if (valPred == valMe)
							return true;
						else
							return false;
					case LessThan:
						if (valPred > valMe)
							return true;
						else
							return false;
					case LessThanOrEqualsTo:
						if (valPred >= valMe)
							return true;
						else
							return false;
				}
				break;
			default:
				break;
		}
		
		return satisfies;
	}
	
	public String toString(){
		
		String ret = "";
		
		switch (this.getCompOperator()){
			case GreaterThan:
				ret += " > ";
				break;
			case GreaterThanOrEqualsTo:
				ret += " >= ";
				break;
			case LessThan:
				ret += " < ";
				break;
			case LessThanOrEqualsTo:
				ret += " <= ";
				break;
			case EqualsTo:
				ret += " = ";
				break;
			case NotEqualsTo:
				ret += " != ";
				break;
		}
		
		ret += this._value;
		
		return ret;
		
	}
}

package uff.dew.svp.algebra.operators.functions;

import uff.dew.svp.algebra.operators.AbstractOperator;

public abstract class FunctionOperator extends AbstractOperator {

	public FunctionOperator() {
		super();
	}
	
	public static FunctionOperator buildFunction(String funcName){
		if (funcName.equals("FuncCount"))
			return new FunctionCountOperator();
		else if (funcName.equals("FuncMax"))
			return new FunctionMaxOperator();
		else if (funcName.equals("FuncMin"))
			return new FunctionMinOperator();
		else if (funcName.equals("FuncSum"))
			return new FunctionSumOperator();
		else if (funcName.equals("FuncAverage"))
			return new FunctionAverageOperator();
		else
			return null;
	}
}

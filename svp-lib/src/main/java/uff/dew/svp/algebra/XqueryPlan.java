package uff.dew.svp.algebra;

import uff.dew.svp.algebra.operators.AbstractOperator;

public class XqueryPlan {
	
	protected String xqueryStr;
	protected AbstractOperator rootOperator;

	/**
	 * Construtor default. Cria o plano global de execuo de uma XQuery.
	 * @param xquery
	 */
	public XqueryPlan(String xquery) {
		this.xqueryStr = xquery;
	}

	public AbstractOperator getRootOperator() {
		return rootOperator;
	}

	public void setRootOperator(AbstractOperator rootOperator) {
		this.rootOperator = rootOperator;
	}


	public String getXqueryStr() {
		return xqueryStr;
	}	
	
	public String toString(){
		return this.rootOperator.toString();
	}
}

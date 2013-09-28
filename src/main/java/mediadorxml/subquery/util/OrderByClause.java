package mediadorxml.subquery.util;

import java.util.ArrayList;

public class OrderByClause {
	
	private ArrayList<String> orderTerms;

	public OrderByClause() {
		this.orderTerms = new ArrayList<String>();
	}

	public ArrayList<String> getOrderTerms() {
		return orderTerms;
	}

	public void addOrderTerm(String orderTerm) {
		this.orderTerms.add(orderTerm);
	}

	public String toString(){
		if (this.orderTerms.size() > 0){
			String ret = "order by ";
			for (int i=0; i<this.orderTerms.size(); i++){
				ret += this.orderTerms.get(i);
				if (i < this.orderTerms.size() - 1)
					ret += ", ";
			}
			ret += " \r\n";
			return ret;
		}
		else
			return "";
	}
	
}

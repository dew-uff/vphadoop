package uff.dew.vphadoop;

public class QuickStart {
	
	public static void main(String[] args) throws Exception {

		BaseXClient client = new BaseXClient("192.168.56.101", 1984,
				"admin", "admin");
		
		String cardinalityQuery = "let $elm := doc('#')/? return count($elm)";
		cardinalityQuery = cardinalityQuery.replace("#", "standard");
		cardinalityQuery = cardinalityQuery.replace("?", "site/regions/*/item/name");
		
		client.executeXQuery(cardinalityQuery, System.out);
		System.out.flush();

		client.close();
	}
}
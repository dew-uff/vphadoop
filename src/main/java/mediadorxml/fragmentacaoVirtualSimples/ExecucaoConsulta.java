package mediadorxml.fragmentacaoVirtualSimples;

import java.io.IOException;

import javax.xml.xquery.XQException;

import uff.dew.vphadoop.catalog.Catalog;
import uff.dew.vphadoop.db.Database;

public class ExecucaoConsulta {

	public static String executeQuery(String xquery) throws IOException {
		
	    try{
	        
	        Database db = Catalog.get().getDatabase();
	    
	        return db.executeQueryAsString(xquery);
			
		} catch (XQException e) {
			System.out.println("ExecucaoConsulta class: Erro ao executar XQuery.");
			e.printStackTrace();
			return null;
		}
//		finally {
//			try {
//				if (xqr!=null) xqr.close();			
//				if (xqe!=null) xqe.close();			
//				if (xqc!=null) xqc.close();				
//			} catch (Exception e2) {
//				System.out.println("ExecucaoConsulta class: Erro ao fechar conexo.");
//				e2.printStackTrace();
//				return null;
//			}
//		}		
		
	}
}



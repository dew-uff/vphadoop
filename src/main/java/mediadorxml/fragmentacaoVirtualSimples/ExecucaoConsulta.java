package mediadorxml.fragmentacaoVirtualSimples;

import java.io.IOException;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQExpression;
import javax.xml.xquery.XQResultSequence;

import uff.dew.vphadoop.db.Catalog;

public class ExecucaoConsulta {

	public static String executeQuery(String xquery) throws IOException {
		
		XQResultSequence xqr = null;
		XQExpression xqe = null;
		XQConnection xqc = null;		
		String retorno = "";		
		
		try {
		    XQDataSource ds = Catalog.get().getDataSource();
			//ConnectionSedna con = new ConnectionSedna();
			xqc = ds.getConnection();			
			xqe = xqc.createExpression();
			xqr = xqe.executeQuery(xquery);	
			
			if (!xqr.next()){				
				return "";
			}			
			
			do {				
				retorno = retorno + xqr.getItemAsString(null);														
			} while (xqr.next());
						
			xqc.close();
			return retorno;
			
		} catch (XQException e) {
			System.out.println("ExecucaoConsulta class: Erro ao executar XQuery.");
			e.printStackTrace();
			return null;
		}
		finally {
			try {
				if (xqr!=null) xqr.close();			
				if (xqe!=null) xqe.close();			
				if (xqc!=null) xqc.close();				
			} catch (Exception e2) {
				System.out.println("ExecucaoConsulta class: Erro ao fechar conexo.");
				e2.printStackTrace();
				return null;
			}
		}		
		
	}
}



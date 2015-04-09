package uff.dew.svp.engine;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import uff.dew.svp.algebra.util.IdGenerator;
import uff.dew.svp.engine.flworprocessor.FLWOR;
import uff.dew.svp.exceptions.AlgebraParserException;
import uff.dew.svp.exceptions.FragmentReductionException;
import uff.dew.svp.exceptions.OptimizerException;
import uff.dew.svp.javaccparser.SimpleNode;
import uff.dew.svp.javaccparser.XQueryParser;

public class XQueryEngine {
	
	protected transient StringBuilder xqueryResultStr;
	protected transient ArrayList<FLWOR> flworList;
		
	public XQueryEngine(){
		this.flworList = new ArrayList<FLWOR>();
	}
	
	public void execute(String xquery) throws XQueryEngineException {
		this.execute(xquery, false);
	}
	
	public void execute(String xquery, boolean debug) throws XQueryEngineException {
		
		IdGenerator.reset();
		
		this.xqueryResultStr = new StringBuilder();
			
		try{
			// Parser da XQuery			
			XQueryParser xqParsed = new XQueryParser(new StringReader(xquery));
			SimpleNode node = xqParsed.Start();
			
			// Processamento e execucao da XQuery
			this.processSimpleNode(node, debug);
		}
		catch(Exception exc){
			throw new XQueryEngineException(exc);
		}
	}
	
	protected void processSimpleNode(final SimpleNode node, final boolean debug) 
		throws OptimizerException, FragmentReductionException, AlgebraParserException, IOException{
		
		if (debug)
			System.out.println(node.toString() + " - " + node.getText() + "\r\n");
		//
		boolean processChild = true;
		
		String element = node.toString();
		if (element == "ElmtConstructor"){
			this.xqueryResultStr.append("<");
		}
		else if (element == "QName"){
			this.xqueryResultStr.append(node.getText());
			this.xqueryResultStr.append(">\r\n");
		}
		else if (element == "ElmtContent"){
			//
		}
		else if (element == "EndTag"){
			this.xqueryResultStr.append("</");
		}
		else if (element == "FLWORExpr"){
			
			// Compilao e execuo do FLWOR
			FLWOR flwor = new FLWOR();
			
			// Compilar o FLWOR
			flwor.compile(node, debug);
						
			this.flworList.add(flwor);
			
			if (debug)
				System.out.println(flwor.toString());
			
			processChild = false;
		}
			
		if (processChild & (node.jjtGetNumChildren()>0)){
			for (int i=0; i<node.jjtGetNumChildren(); i++){
				this.processSimpleNode((SimpleNode)node.jjtGetChild(i), debug);
			}
		}
	}
	
	public ArrayList<FLWOR> getFlworList(){
		return this.flworList;
	}
	
}

package mediadorxml.engine;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import mediadorxml.algebra.util.IdGenerator;
import mediadorxml.engine.flworprocessor.FLWOR;
import mediadorxml.exceptions.AlgebraParserException;
import mediadorxml.exceptions.FragmentReductionException;
import mediadorxml.exceptions.OptimizerException;
import mediadorxml.javaccparser.SimpleNode;
import mediadorxml.javaccparser.XQueryParser;

public class XQueryEngine {
	
	protected transient StringBuilder xqueryResultStr;
	protected transient ArrayList<FLWOR> flworList;
		
	public XQueryEngine(){
		this.flworList = new ArrayList<FLWOR>();
	}
	
	//TODO Customize this Exception
	public void execute(String xquery) throws IOException {
		this.execute(xquery, false);
	}
	
	public void execute(String xquery, boolean debug) throws IOException {
		
		IdGenerator.reset();
		
		this.xqueryResultStr = new StringBuilder();
			
		try{
			
			long startParseTime = System.nanoTime();
						
			// Parser da XQuery			
			XQueryParser xqParsed = new XQueryParser(new StringReader(xquery));
			SimpleNode node = xqParsed.Start();
			
			long parseTime = (System.nanoTime() - startParseTime)/1000000;
			
			// Processamento e execuo da XQuery
			this.processSimpleNode(node, debug);
			
				
		}
		catch(Exception exc){
			throw new IOException(exc);
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
	
	public ArrayList getFlworList(){
		return this.flworList;
	}
	
}

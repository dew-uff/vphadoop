1) Excluir todos os arquivos de mediador.javaccparser exceto este readme.txt, 
	runJavaCC.bat e xquery.jjt
	
2) Executar o runJavaCC.bat

3) incluir o código abaixo na classe SimpleNode

------------------------------------------

  protected int _type;
  protected String _text;

  public SimpleNode(int i) {
    id = i;
  }
  
  public void setToken(int i, String t){
	  this._type = i;
	  this._text = t;
  }
  
  public int getType(){
	  return _type;
  }
  public String getText(){
	  return _text;
  }
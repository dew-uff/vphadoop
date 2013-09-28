package mediadorxml.fragmentacaoVirtualSimples;

public class Index {

	protected String name; // nome do ndice.	
	protected String objectType; // indica se o ndice est aplicado sobre uma coleo ou sobre um documento.
	protected String objectName; // nome da coleo ou do documento sobre o qual o ndice foi definido.
	protected String onPath; // caminho xpath at o nodo pai do nodo indexado.
	protected String byPath; // nome do nodo indexado.
	protected String asType; // indica o tipo do elemento. Pode assumir os valores xs:string,xs:double,xs:integer,...
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getObjectType() {
		return objectType;
	}
	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}
	public String getObjectName() {
		return objectName;
	}
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
	public String getOnPath() {
		return onPath;
	}
	public void setOnPath(String onPath) {
		this.onPath = onPath;
	}
	public String getByPath() {
		return byPath;
	}
	public void setByPath(String byPath) {
		this.byPath = byPath;
	}
	public String getAsType() {
		return asType;
	}
	public void setAsType(String asType) {
		this.asType = asType;
	}
}

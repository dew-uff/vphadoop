package uff.dew.svp.fragmentacaoVirtualSimples;

import java.util.ArrayList;

public class Reference {

	protected ArrayList<ReferencedBy> referencedBy; // elementos que fazem referncia(foreign key) a este
	public ArrayList<ReferencedBy> getReferencedBy() {
		return referencedBy;
	}
	public void setReferencedBy(ArrayList<ReferencedBy> referencedBy) {
		this.referencedBy = referencedBy;
	}
	protected String path; // caminho do elemento referenciado (primary key)
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	
}

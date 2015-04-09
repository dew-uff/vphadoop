package uff.dew.svp.catalog.util;

public class View {

	protected String viewName;
	protected String xsdFile; // Nome do arquivo com o schema da viso local
	
	public String getViewName() {
		return viewName;
	}

	public void setViewName(String name) {
		viewName = name;
	}

	public String getXsdFile() {
		return xsdFile;
	}

	public void setXsdFile(String file) {
		xsdFile = file;
	}
}

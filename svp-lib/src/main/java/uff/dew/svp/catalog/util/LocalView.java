package uff.dew.svp.catalog.util;

import java.util.ArrayList;

public class LocalView extends View {
	
	public enum FragmentType {FULL, HORIZONTAL, VERTICAL, HYBRID};
	
	protected ArrayList<WrapperLocation> wrapperLocation;      // Localizao da view local
	protected String referenceCollection;	  // Colleo sobre a qual o predicado  aplicado
	protected ArrayList<String> selectionPredicates;  // Predicados de seleo que define a formao do fragmento
	protected ArrayList<String> projectionPredicates; // Predicados de projeo
	protected FragmentType type;			  // Tipo de fragmento
	protected int totalNodes;
	
	public LocalView(){
		this.wrapperLocation = new ArrayList<WrapperLocation>();
		this.selectionPredicates = new ArrayList<String>();
		this.projectionPredicates = new ArrayList<String>();
	}

	public int getTotalNodes() {
		return totalNodes;
	}
	public void setTotalNodes(int totalNodes) {
		this.totalNodes = totalNodes;
	}
	public ArrayList<String> getProjectionPredicates() {
		return projectionPredicates;
	}
	public void setProjectionPredicates(ArrayList<String> projectionPredicates) {
		this.projectionPredicates = projectionPredicates;
	}
	public String getReferenceCollection() {
		return referenceCollection;
	}
	public void setReferenceCollection(String referenceCollection) {
		this.referenceCollection = referenceCollection;
	}
	public ArrayList<String> getSelectionPredicates() {
		return selectionPredicates;
	}
	public void setSelectionPredicates(ArrayList<String> selectionPredicates) {
		this.selectionPredicates = selectionPredicates;
	}
	public void setSelectionPredicates(String selectionPredicate) {
		this.selectionPredicates = new ArrayList<String>();
		this.selectionPredicates.add(selectionPredicate);
	}
	public ArrayList<WrapperLocation> getWrapperLocation() {
		return wrapperLocation;
	}
	public WrapperLocation getWrapperLocation(String siteURI){
		for (int i=0; i<this.wrapperLocation.size(); i++){
			WrapperLocation wl = this.wrapperLocation.get(i);
			if (wl.getUri().equals(siteURI)){
				return wl;
			}
		}
		return null;
	}
	public void setWrapperLocation(ArrayList<WrapperLocation> wrapperLocation) {
		this.wrapperLocation = wrapperLocation;
	}
	public void setWrapperLocation(WrapperLocation wrapperLocation) {
		this.wrapperLocation = new ArrayList<WrapperLocation>();
		this.wrapperLocation.add(wrapperLocation);
	}
	public FragmentType getFragmentType(){
		if ((this.selectionPredicates.size() == 0)
				&& (this.projectionPredicates.size() == 0)){
			return FragmentType.FULL;
		}
		else if ((this.selectionPredicates.size() > 0)
				&& (this.projectionPredicates.size() == 0)){
			return FragmentType.HORIZONTAL;
		}
		else if ((this.selectionPredicates.size() == 0)
				&& (this.projectionPredicates.size() > 0)){
			return FragmentType.VERTICAL;
		}
		else 
			return FragmentType.HYBRID;
	}	
}

package uff.dew.svp.catalog.util;

import java.util.ArrayList;

public class GlobalView extends View {

	protected ArrayList<LocalView> localViews;
	protected ArrayList<String> indexNodes;
	
	public GlobalView(){
		this.localViews = new ArrayList<LocalView>();
	}
	
	public GlobalView(String viewName){
		this();
		this.viewName = viewName;
	}

	//public LocalView[] getLocalViews() {
	public ArrayList<LocalView> getLocalViews() {
		return localViews;
	}

	public void setLocalViews(ArrayList<LocalView> views) {
		localViews = views;
	}
	
	public void addLocalView(LocalView lv){
		this.localViews.add(lv);
	}

	public ArrayList<String> getIndexNodes() {
		return indexNodes;
	}

	public void setIndexNodes(ArrayList<String> indexNodes) {
		this.indexNodes = indexNodes;
	}
	
}
